import subprocess as sb
from kafka import KafkaProducer
from datetime import datetime
import os, time, csv, random, json, sys, traceback
from kafka.admin import KafkaAdminClient, NewTopic
import kafka.errors as KafkaError
import pandas as pd

bootstrap_servers = ["localhost:9092"]
admin_client = KafkaAdminClient(bootstrap_servers = bootstrap_servers)
topic_name = "user_order_activity_stream"
topic = NewTopic(topic_name,num_partitions = 3, replication_factor = 1)
try:
    existing_topics = admin_client.list_topics()
    print(f"existing_topics in the broker: {existing_topics}")
    if topic_name not in existing_topics:
        admin_client.create_topics([topic])
        print(f"{topic_name} created successfully")
except KafkaError as e:
    print(e)
admin_client.close()

# Start Zookeeper and Kafka

try:
    cwd_= os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    dataset_path = cwd_+ "/dataset/bittlingmayer/amazonreviews/order_products__prior.csv"
    offset_file=cwd_+ "/dataset/bittlingmayer/amazonreviews/order_products__prior_offset.txt"
    if len(sys.argv)>1:
        chunk_size = int(sys.argv[1])
    else:
        chunk_size = random.randint(1,450)
    if os.path.exists(offset_file):
        with open(offset_file, 'r') as f:
            last_offset = int(f.read().strip())
    else:
        last_offset = 0  # If the offset file doesn't exist, start from the beginning
    if os.path.exists(dataset_path):
        chunk = pd.read_csv(dataset_path, skiprows=range(1, last_offset + 1), nrows=chunk_size)
        print(chunk.columns)
        chunk = chunk.to_dict('records')
        new_offset = last_offset + len(chunk)
        with open(offset_file, 'w') as f:
            f.write(str(new_offset))
    producer = KafkaProducer(
        bootstrap_servers = ["localhost:9092"],
        value_serializer = lambda v: json.dumps(v).encode("utf-8"),
        acks=1,  # Ensure quicker acknowledgment from Kafka broker
        request_timeout_ms=5000  # Set a timeout (e.g., 5 seconds)
    )
    producer.send("user_order_activity_stream",chunk)
    print(f"{len(chunk)} records successfully send over the topic at {datetime.now()}")
    producer.flush()
    producer.close()

except Exception as e:
    traceback.print_exc()

