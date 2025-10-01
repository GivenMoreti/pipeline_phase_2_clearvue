from pymongo import MongoClient
from confluent_kafka import Consumer
import json

def diagnose_system():
    print("🔧 CLEARVUE PAYMENT SYSTEM DIAGNOSTIC")
    print("=" * 50)
    
    # Check MongoDB
    print("\n1. 📊 MONGODB CHECK:")
    try:
        client = MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=5000)
        client.server_info()  # Force connection
        print("   ✅ MongoDB is running")
        
        db = client['clearvue']
        collections = db.list_collection_names()
        print(f"   ✅ Database 'clearvue' accessible")
        print(f"   📁 Collections: {collections}")
        
        if 'payment lines' in collections:
            count = db["payment lines"].count_documents({})
            print(f"   📈 payment lines count: {count} documents")
            
            # Show latest 3 payments
            latest = list(db["payment lines"].find().sort('_id', -1).limit(3))
            if latest:
                print("   🆕 Latest payments:")
                for doc in latest:
                    print(f"      - {doc['CUSTOMER_NUMBER']} | ${doc['TOT_PAYMENT']} | {doc['DEPOSIT_REF']}")
            else:
                print("   ℹ️  No payments in collection")
        else:
            print("   ❌ payment lines collection not found")
            
    except Exception as e:
        print(f"   ❌ MongoDB connection failed: {e}")
    
    # Check Kafka
    print("\n2. 🔄 KAFKA CHECK:")
    try:
        consumer = Consumer({
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'diagnostic-group',
            'auto.offset.reset': 'earliest'
        })
        
        # Try to get metadata
        metadata = consumer.list_topics(timeout=5)
        print("   ✅ Kafka is running")
        print(f"   📋 Available topics: {list(metadata.topics.keys())}")
        
        if 'clearvue_payments' in metadata.topics:
            topic_metadata = metadata.topics['clearvue_payments']
            print(f"   ✅ 'clearvue_payments' topic exists")
            print(f"   📊 Partitions: {len(topic_metadata.partitions)}")
            
            # Check message count in topic
            consumer.subscribe(['clearvue_payments'])
            msg = consumer.poll(1.0)
            if msg:
                print("   📨 Messages are available in topic")
            else:
                print("   ℹ️  No unread messages in topic (might be consumed already)")
        else:
            print("   ❌ 'clearvue_payments' topic not found")
            
        consumer.close()
        
    except Exception as e:
        print(f"   ❌ Kafka connection failed: {e}")
    
    print("\n3. 💡 RECOMMENDED ACTIONS:")
    print("   - If MongoDB has 0 payments: Start the consumer script")
    print("   - If Kafka connection fails: Check if Docker containers are running")
    print("   - Run: docker-compose ps (to check Kafka status)")
    
    print("\n" + "=" * 50)

if __name__ == "__main__":
    diagnose_system()