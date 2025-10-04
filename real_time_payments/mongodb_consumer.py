import json
import time
from confluent_kafka import Consumer, KafkaError
from pymongo import MongoClient
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ClearVuePaymentConsumer:
    def __init__(self, bootstrap_servers='localhost:9092', 
                 mongo_uri='mongodb://localhost:27017/'):
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
        self.db = self.client['clearvue_2']
        self.collection = self.db['payment_lines']
        
        # Create indexes
        try:
            self.collection.create_index([('CUSTOMER_NUMBER', 1)])
            self.collection.create_index([('FIN_PERIOD', 1)])
            self.collection.create_index([('DEPOSIT_DATE', -1)])
            self.collection.create_index([('DEPOSIT_REF', 1)], unique=True)
        except Exception as idx_err:
            logger.warning(f"Index creation warning: {idx_err}")
        
        logger.info("ClearVue Payment Consumer initialized")
    
    def enrich_payment_data(self, payment_event):
        """Enrich the payment data with derived fields"""
        if 'DEPOSIT_DATE' in payment_event and payment_event['DEPOSIT_DATE']:
            try:
                if isinstance(payment_event['DEPOSIT_DATE'], str):
                    payment_event['DEPOSIT_DATE'] = datetime.fromisoformat(payment_event['DEPOSIT_DATE'])
                elif not isinstance(payment_event['DEPOSIT_DATE'], datetime):
                    payment_event['DEPOSIT_DATE'] = datetime.fromisoformat(str(payment_event['DEPOSIT_DATE']))
            except Exception:
                logger.debug("Could not parse DEPOSIT_DATE, leaving as-is")

        payment_event['PROCESSED_AT'] = datetime.utcnow()
        payment_event['KAFKA_INGESTED_AT'] = datetime.utcnow()
        payment_event['DATA_SOURCE'] = 'kafka_stream'

        # Calculate discount rate
        try:
            payment_event['DISCOUNT_RATE'] = round(
                (payment_event.get('DISCOUNT', 0) / payment_event.get('BANK_AMT', 0)) * 100, 2
            ) if payment_event.get('BANK_AMT', 0) > 0 else 0
        except Exception:
            payment_event['DISCOUNT_RATE'] = 0

        # Categorize payment
        payment_event['PAYMENT_CATEGORY'] = self._categorize_payment(payment_event.get('TOT_PAYMENT', 0))

        # Extract financial year and quarter from yyyymm format
        if 'FIN_PERIOD' in payment_event:
            fin_period = payment_event['FIN_PERIOD']
            try:
                # Parse yyyymm format (e.g., "202508")
                year = int(fin_period[:4])
                month = int(fin_period[4:6])
                payment_event['FIN_YEAR'] = year
                payment_event['FIN_MONTH'] = month
                payment_event['FIN_QUARTER'] = (month - 1) // 3 + 1
                payment_event['MONTH_NAME'] = datetime(year, month, 1).strftime('%B')
            except Exception:
                now = datetime.now()
                payment_event['FIN_YEAR'] = now.year
                payment_event['FIN_MONTH'] = now.month
                payment_event['FIN_QUARTER'] = (now.month - 1) // 3 + 1
                payment_event['MONTH_NAME'] = now.strftime('%B')

    def _categorize_payment(self, amount):
        """Categorize payment amount for analytics"""
        try:
            amount = float(amount)
        except Exception:
            amount = 0

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
                    # Parse message
                    payment_event = json.loads(msg.value().decode('utf-8'))

                    # Basic validation
                    if 'DEPOSIT_REF' not in payment_event:
                        logger.warning("Message missing DEPOSIT_REF; skipping")
                        continue

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
                                f"Customer: {payment_event.get('CUSTOMER_NUMBER', 'N/A')} | "
                                f"Amount: ${payment_event.get('TOT_PAYMENT', 0):,.2f} | "
                                f"Period: {payment_event.get('FIN_PERIOD', 'N/A')}")

                    # Print stats every 5 messages
                    if message_count % 5 == 0:
                        self.print_payment_stats()

                except Exception as e:
                    logger.exception(f"Error processing message: {str(e)}")

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