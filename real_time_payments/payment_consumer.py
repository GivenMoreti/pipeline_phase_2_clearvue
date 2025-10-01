import json
import time
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
from faker import Faker

class ClearVuePaymentProducer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda v: v.encode('utf-8') if v else None
        )
        self.topic = 'clearvue_payments'
        self.fake = Faker()
        
        # Pre-defined customer numbers from your actual data
        self.customer_numbers = [
            'CUST001', 'CUST002', 'CUST003', 'CUST004', 'CUST005',
            'CUST006', 'CUST007', 'CUST008', 'CUST009', 'CUST010'
        ]
        
    def calculate_financial_period(self, date):
        """Calculate ClearVue's financial period based on your business rules"""
        # This implements your financial year logic:
        # Financial month starts last Saturday of previous month, ends last Friday of current month
        
        year = date.year
        month = date.month
        
        # Simple implementation - you can enhance this with your exact logic
        quarter = (month - 1) // 3 + 1
        fin_period = f"{year}-Q{quarter}"
        
        return fin_period
    
    def generate_payment_event(self):
        """Generate a payment event matching your actual field structure"""
        
        # Random date within last 90 days
        deposit_date = self.fake.date_between(start_date='-90d', end_date='today')
        
        # Calculate financial period
        fin_period = self.calculate_financial_period(deposit_date)
        
        # Generate realistic payment amounts
        bank_amt = round(random.uniform(100.0, 5000.0), 2)
        discount = round(bank_amt * random.uniform(0.0, 0.15), 2)  # 0-15% discount
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
        """Start producing payment events at specified interval (seconds)"""
        print(f"Starting ClearVue Payment Producer...")
        print(f"Fields: CUSTOMER_NUMBER, FIN_PERIOD, DEPOSIT_DATE, DEPOSIT_REF, BANK_AMT, DISCOUNT, TOT_PAYMENT")
        print(f"Sending events every {interval} seconds...")
        print("Press Ctrl+C to stop.\n")
        
        event_count = 0
        try:
            while True:
                payment_event = self.generate_payment_event()
                
                # Use customer number as key for partitioning
                self.producer.send(
                    topic=self.topic,
                    key=payment_event['CUSTOMER_NUMBER'],
                    value=payment_event
                )
                
                event_count += 1
                print(f"Event #{event_count}: {payment_event['CUSTOMER_NUMBER']} | "
                      f"{payment_event['DEPOSIT_REF']} | "
                      f"${payment_event['TOT_PAYMENT']:,.2f} | "
                      f"{payment_event['FIN_PERIOD']}")
                
                # Flush to ensure message is sent
                self.producer.flush()
                
                time.sleep(interval)
                
        except KeyboardInterrupt:
            print(f"\nStopping producer. Sent {event_count} events.")
        finally:
            self.producer.close()

if __name__ == "__main__":
    producer = ClearVuePaymentProducer()
    producer.start_producing(interval=4)  # Send event every 4 seconds