
import json
import time
import random
from datetime import datetime
from confluent_kafka import Producer
from faker import Faker
import socket

class ClearVuePaymentProducer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.conf = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': socket.gethostname(),
            'message.timeout.ms': 5000,
            'retries': 3
        }
        self.producer = Producer(self.conf)
        self.topic = 'clearvue_payments'
        self.fake = Faker()
        
        self.customer_numbers = [
            'CUST001', 'CUST002', 'CUST003', 'CUST004', 'CUST005',
            'CUST006', 'CUST007', 'CUST008', 'CUST009', 'CUST010'
        ]
        
    def delivery_report(self, err, msg):
        """Called once for each message produced to indicate delivery result."""
        if err is not None:
            print(f'Message delivery failed: {err}')
        else:
            print(f'Message delivered to {msg.topic()} [{msg.partition()}]')
        
    def calculate_financial_period(self, date):
        """Calculate ClearVue's financial period"""
        year = date.year
        month = date.month
        quarter = (month - 1) // 3 + 1
        fin_period = f"{year}-Q{quarter}"
        return fin_period
    
    def generate_payment_event(self):
        """Generate a payment event matching your actual field structure"""
        deposit_date = self.fake.date_between(start_date='-90d', end_date='today')
        fin_period = self.calculate_financial_period(deposit_date)
        
        bank_amt = round(random.uniform(100.0, 5000.0), 2)
        discount = round(bank_amt * random.uniform(0.0, 0.15), 2)
        tot_payment = bank_amt - discount
        
        payment_event = {
            'CUSTOMER_NUMBER': random.choice(self.customer_numbers),
            'FIN_PERIOD': fin_period,
            'DEPOSIT_DATE': deposit_date.isoformat(),
            'DEPOSIT_REF': f"DEP{self.fake.unique.random_int(100000, 999999)}",
            'BANK_AMT': bank_amt,
            'DISCOUNT': discount,
            'TOT_PAYMENT': tot_payment,
            'PROCESSED_AT': datetime.utcnow().isoformat()
        }
        
        return payment_event
    
    def start_producing(self, interval=5):
        """Start producing payment events"""
        print(f"Starting ClearVue Payment Producer...")
        print(f"Fields: CUSTOMER_NUMBER, FIN_PERIOD, DEPOSIT_DATE, DEPOSIT_REF, BANK_AMT, DISCOUNT, TOT_PAYMENT")
        print(f"Sending events every {interval} seconds...")
        print("Press Ctrl+C to stop.\n")
        
        event_count = 0
        try:
            while True:
                payment_event = self.generate_payment_event()
                
                # Produce message
                self.producer.produce(
                    topic=self.topic,
                    key=payment_event['CUSTOMER_NUMBER'],
                    value=json.dumps(payment_event),
                    callback=self.delivery_report
                )
                
                event_count += 1
                print(f"Event #{event_count}: {payment_event['CUSTOMER_NUMBER']} | "
                      f"{payment_event['DEPOSIT_REF']} | "
                      f"${payment_event['TOT_PAYMENT']:,.2f} | "
                      f"{payment_event['FIN_PERIOD']}")
                
                # Poll for callbacks
                self.producer.poll(0)
                
                time.sleep(interval)
                
        except KeyboardInterrupt:
            print(f"\nStopping producer. Sent {event_count} events.")
        finally:
            # Wait for any outstanding messages to be delivered
            self.producer.flush()

if __name__ == "__main__":
    producer = ClearVuePaymentProducer()
    producer.start_producing(interval=4)