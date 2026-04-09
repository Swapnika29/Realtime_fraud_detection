import os
import json
import time
import pandas as pd
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

def commit_callback(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")

def main():
    broker = os.environ.get("KAFKA_BROKER", "localhost:9092")
    topic_name = "financial_transactions"
    
    # Wait for Kafka to be ready
    print("Waiting for Kafka to start...")
    time.sleep(20)
    
    print(f"Connecting to Kafka broker: {broker}")
    admin_client = AdminClient({'bootstrap.servers': broker})
    
    print(f"Attempting to create topic {topic_name}...")
    new_topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)
    admin_client.create_topics([new_topic])

    producer = Producer({'bootstrap.servers': broker})
    
    import glob
    csv_files = glob.glob("/app/data/*.csv")
    source_files = [f for f in csv_files if "anomalies.csv" not in f]
    
    if not source_files:
        print(f"ERROR: Dataset not found in /app/data/.")
        print("Please ensure you placed your CSV dataset in the 'data' directory.")
        time.sleep(60)
        return
        
    data_path = source_files[0]
    print(f"Starting to produce messages to {topic_name} from {data_path}...")
    chunk_size = 500
    
    # Read CSV in chunks
    for chunk in pd.read_csv(data_path, chunksize=chunk_size):
        for index, row in chunk.iterrows():
            payload = row.to_dict()
            producer.produce(
                topic_name, 
                value=json.dumps(payload).encode('utf-8'),
                callback=commit_callback
            )
            producer.poll(0)
            
            # Simulate streaming delay
            time.sleep(0.01)
            
        producer.flush()
        print(f"Produced batch of {chunk_size} records...")

if __name__ == "__main__":
    main()
