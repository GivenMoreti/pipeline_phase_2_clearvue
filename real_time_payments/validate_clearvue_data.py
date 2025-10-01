from pymongo import MongoClient
from datetime import datetime
import pandas as pd

def validate_clearvue_data():
    client = MongoClient('mongodb://localhost:27017/')
    db = client['clearvue']
    collection = db['payment lines']
    
    print("=== ClearVue Payment Data Validation ===")
    
    # Basic counts
    total_count = collection.count_documents({})
    print(f"\n1. Total Payment Records: {total_count}")
    
    if total_count == 0:
        print("No data found. Please run the simulation first.")
        return
    
    # Customer distribution
    print("\n2. Payments by Customer:")
    pipeline = [
        {'$group': {'_id': '$CUSTOMER_NUMBER', 'count': {'$sum': 1}, 'total_amount': {'$sum': '$TOT_PAYMENT'}}},
        {'$sort': {'total_amount': -1}}
    ]
    customer_stats = list(collection.aggregate(pipeline))
    for cust in customer_stats:
        print(f"   {cust['_id']}: {cust['count']} payments, Total: ${cust['total_amount']:,.2f}")
    
    # Financial period summary
    print("\n3. Payments by Financial Period:")
    pipeline = [
        {'$group': {'_id': '$FIN_PERIOD', 'count': {'$sum': 1}, 'total_amount': {'$sum': '$TOT_PAYMENT'}}},
        {'$sort': {'_id': 1}}
    ]
    period_stats = list(collection.aggregate(pipeline))
    for period in period_stats:
        print(f"   {period['_id']}: {period['count']} payments, Total: ${period['total_amount']:,.2f}")
    
    # Amount analysis
    print("\n4. Payment Amount Analysis:")
    pipeline = [
        {'$group': {
            '_id': None,
            'total_bank_amt': {'$sum': '$BANK_AMT'},
            'total_discount': {'$sum': '$DISCOUNT'},
            'total_payments': {'$sum': '$TOT_PAYMENT'},
            'avg_payment': {'$avg': '$TOT_PAYMENT'},
            'max_payment': {'$max': '$TOT_PAYMENT'},
            'min_payment': {'$min': '$TOT_PAYMENT'}
        }}
    ]
    amount_stats = list(collection.aggregate(pipeline))[0]
    print(f"   Total Bank Amount: ${amount_stats['total_bank_amt']:,.2f}")
    print(f"   Total Discount: ${amount_stats['total_discount']:,.2f}")
    print(f"   Total Payments: ${amount_stats['total_payments']:,.2f}")
    print(f"   Average Payment: ${amount_stats['avg_payment']:,.2f}")
    print(f"   Payment Range: ${amount_stats['min_payment']:,.2f} - ${amount_stats['max_payment']:,.2f}")
    
    # Recent payments sample
    print("\n5. 5 Most Recent Payments:")
    recent = collection.find().sort('PROCESSED_AT', -1).limit(5)
    for payment in recent:
        print(f"   {payment['DEPOSIT_REF']} | {payment['CUSTOMER_NUMBER']} | "
              f"${payment['TOT_PAYMENT']:,.2f} | {payment['FIN_PERIOD']}")
    
    client.close()

if __name__ == "__main__":
    validate_clearvue_data()