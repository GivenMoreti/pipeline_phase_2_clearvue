

import json
import time
from confluent_kafka import Consumer, KafkaError
from pymongo import MongoClient
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ClearVuePaymentConsumer:
    def __init__(self, bootstrap_servers='localhost:9092', mongo_uri='mongodb://localhost:27017/'):
        self.conf = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': 'clearvue-payment-consumer',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True
        }
        self.consumer = Consumer(self.conf)
        self.topic = 'clearvue_payments'
        
        # MongoDB connection
        self.client = MongoClient(mongo_uri)
        self.db = self.client['clearvue']
        self.collection = self.db['payment lines']
        
        # Create indexes
        self.collection.create_index([('CUSTOMER_NUMBER', 1)])
        self.collection.create_index([('FIN_PERIOD', 1)])
        self.collection.create_index([('DEPOSIT_DATE', -1)])
        self.collection.create_index([('DEPOSIT_REF', 1)], unique=True)
        
        logger.info("ClearVue Payment Consumer initialized")
    
    def enrich_payment_data(self, payment_event):
        """Enrich the payment data with derived fields"""
        if 'DEPOSIT_DATE' in payment_event:
            payment_event['DEPOSIT_DATE'] = datetime.fromisoformat(payment_event['DEPOSIT_DATE'])
        
        payment_event['PROCESSED_AT'] = datetime.utcnow()
        payment_event['KAFKA_INGESTED_AT'] = datetime.utcnow()
        payment_event['DATA_SOURCE'] = 'kafka_stream'

        payment_event['DISCOUNT_RATE'] = round(
            (payment_event['DISCOUNT'] / payment_event['BANK_AMT']) * 100, 2
        ) if payment_event['BANK_AMT'] > 0 else 0
        
        payment_event['PAYMENT_CATEGORY'] = self._categorize_payment(payment_event['TOT_PAYMENT'])
        
        if 'FIN_PERIOD' in payment_event:
            fin_period = payment_event['FIN_PERIOD']
            try:
                year, quarter = fin_period.split('-Q')
                payment_event['FIN_YEAR'] = int(year)
                payment_event['FIN_QUARTER'] = int(quarter)
            except:
                payment_event['FIN_YEAR'] = datetime.now().year
                payment_event['FIN_QUARTER'] = (datetime.now().month - 1) // 3 + 1
    
    def _categorize_payment(self, amount):
        """Categorize payment amount for analytics"""
        if amount < 500:
            return 'SMALL'
        elif amount < 2000:
            return 'MEDIUM'
        else:
            return 'LARGE'
    
    def start_consuming(self):
        """Start consuming payment events from Kafka"""
        self.consumer.subscribe([self.topic])
        logger.info("Starting to consume payment events...")
        
        message_count = 0
        try:
            while True:
                msg = self.consumer.poll(1.0)
                
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Kafka error: {msg.error()}")
                        break
                
                try:
                    payment_event = json.loads(msg.value().decode('utf-8'))
                    
                    # Check for duplicate
                    existing = self.collection.find_one({
                        'DEPOSIT_REF': payment_event['DEPOSIT_REF']
                    })
                    
                    if existing:
                        logger.warning(f"Duplicate DEPOSIT_REF skipped: {payment_event['DEPOSIT_REF']}")
                        continue
                    
                    # Enrich and store
                    self.enrich_payment_data(payment_event)
                    result = self.collection.insert_one(payment_event)
                    
                    message_count += 1
                    logger.info(f"Stored payment: {payment_event['DEPOSIT_REF']} | "
                               f"Customer: {payment_event['CUSTOMER_NUMBER']} | "
                               f"Amount: ${payment_event['TOT_PAYMENT']:,.2f}")
                    
                    # Print stats every 5 messages
                    if message_count % 5 == 0:
                        self.print_payment_stats()
                        
                except Exception as e:
                    logger.error(f"Error processing message: {str(e)}")
                    
        except KeyboardInterrupt:
            logger.info("Stopping ClearVue payment consumer...")
        finally:
            self.consumer.close()
            self.client.close()
    
    def print_payment_stats(self):
        """Print current payment statistics"""
        try:
            total_count = self.collection.count_documents({})
            pipeline = [
                {'$group': {
                    '_id': '$FIN_PERIOD',
                    'total_payments': {'$sum': 1},
                    'total_amount': {'$sum': '$TOT_PAYMENT'},
                    'avg_amount': {'$avg': '$TOT_PAYMENT'}
                }},
                {'$sort': {'_id': 1}}
            ]
            
            period_stats = list(self.collection.aggregate(pipeline))
            
            logger.info("=== Payment Collection Stats ===")
            logger.info(f"Total Payments: {total_count}")
            for stat in period_stats:
                logger.info(f"  {stat['_id']}: {stat['total_payments']} payments, "
                           f"Total: ${stat['total_amount']:,.2f}")
                           
        except Exception as e:
            logger.error(f"Error generating stats: {str(e)}")

if __name__ == "__main__":
    consumer = ClearVuePaymentConsumer()
    consumer.start_consuming()