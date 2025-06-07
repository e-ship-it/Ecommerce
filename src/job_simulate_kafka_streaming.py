import subprocess as sb
from kafka import KafkaProducer
from datetime import datetime
import os, time, csv, random, json, sys
from kafka.admin import KafkaAdminClient, NewTopic
import kafka.errors as KafkaError

bootstrap_servers = ["localhost:9092"]
admin_client = KafkaAdminClient(bootstrap_servers = bootstrap_servers)
topic_name = "orders"
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
    dataset_path = cwd_+ "/dataset/bittlingmayer/amazonreviews/orders.csv"
    if os.path.exists(dataset_path):
        with open(dataset_path,'r') as f:
            lines = list(csv.DictReader(f))
    if sys.argv[1]:
        chunk_size = int(sys.argv[1])
    else:
        chunk_size = random.randint(1,50)
    random_records = random.sample(lines,chunk_size)
    print(chunk_size,len(random_records))
    producer = KafkaProducer(
        bootstrap_servers = ["localhost:9092"],
        value_serializer = lambda v: json.dumps(v).encode("utf-8"),
        acks=1,  # Ensure quicker acknowledgment from Kafka broker
        request_timeout_ms=5000  # Set a timeout (e.g., 5 seconds)
    )
    producer.send("orders",random_records)
    print(f"{len(random_records)} records successfully send over the topic at {datetime.now()}")
    producer.flush()
    producer.close()

except Exception as e:
    print(e)

