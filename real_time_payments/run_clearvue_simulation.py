import threading
import time
import subprocess
import sys
import os

def run_producer():
    """Run the payment producer"""
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        producer_script = os.path.join(script_dir, "payment_producer.py")
        subprocess.run([sys.executable, producer_script])
    except KeyboardInterrupt:
        pass

def run_consumer():
    """Run the MongoDB consumer"""
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        consumer_script = os.path.join(script_dir, "mongodb_consumer.py")
        subprocess.run([sys.executable, consumer_script])
    except KeyboardInterrupt:
        pass

def main():
    print("=== ClearVue Ltd. Payment Stream Simulation ===")
    print("Kafka UI: http://localhost:8080")
    print("MongoDB: localhost:27017/clearvue_2")
    print("Collection: payment_lines")
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