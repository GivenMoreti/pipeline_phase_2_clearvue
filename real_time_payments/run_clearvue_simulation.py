
import threading
import time
import subprocess
import sys

def run_producer():
    """Run the fixed payment producer"""
    try:
        subprocess.run([sys.executable, "payment_producer.py"])
    except KeyboardInterrupt:
        pass

def run_consumer():
    """Run the fixed MongoDB consumer"""
    try:
        subprocess.run([sys.executable, "mongodb_consumer.py"])
    except KeyboardInterrupt:
        pass

def main():
    print("=== ClearVue Ltd. Payment Stream Simulation ===")
    print("Kafka UI: http://localhost:8080")
    print("MongoDB: localhost:27017/clearvue")
    print("Collection: payment lines")
    print("\nUsing Confluent Kafka Python library for better stability")
    print("\nPress Ctrl+C to stop simulation\n")
    
    producer_thread = threading.Thread(target=run_producer)
    consumer_thread = threading.Thread(target=run_consumer)
    
    producer_thread.daemon = True
    consumer_thread.daemon = True
    
    producer_thread.start()
    time.sleep(3)
    consumer_thread.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down ClearVue payment simulation...")

if __name__ == "__main__":
    main()