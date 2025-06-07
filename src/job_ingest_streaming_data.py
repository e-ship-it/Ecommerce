import pandas as pd
from kafka import KafkaConsumer
from dotenv import load_dotenv
import psycopg2, os, json, csv, time
from pathlib import Path
from kafka.admin import KafkaAdminClient
from psycopg2.extras import execute_batch

def load_data_from_kafka_into_postgres(data,cursor,connection):
    values_to_insert = [(d['order_id'],d['user_id'],d['eval_set'],d['order_number'],d['order_dow'],d['order_hour_of_day'],d['days_since_prior_order'])
    for d in data]
    print(values_to_insert)
    insert_query = """
    INSERT INTO streams.orders (order_id,user_id,eval_set,order_number,order_dow,order_hour_of_day,days_since_prior_order)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    try:
        execute_batch(cursor,insert_query,values_to_insert)
        print("Data inserted successfully.")
    except Exception as e:
        print(f"Error inserting data: {e}")
        connection.rollback()

def read_from_postgres(cursor):
    print(cursor.execute("SELECT * FROM streams.orders"))

admin_client = KafkaAdminClient()
print(admin_client.list_topics())

# Step 1: Load environment variables from .env file
load_dotenv(dotenv_path = Path('dev.env'))
# Step 2: Get the connection parameters from the environment
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")

# connect to postgres DB
connection = psycopg2.connect(
    dbname = db_name,
    user = db_user,
    password = db_password,
    host = db_host,
    port = db_port
)

# Enable auto-commit mode
connection.autocommit = True # for postgrs transactions
cursor = connection.cursor() 


consumer = KafkaConsumer(
    "orders",
    bootstrap_servers = ['localhost:9092'],
    auto_offset_reset='earliest',  # Start from the earliest message (default: 'latest')
    group_id='orders-group', #mandate
    value_deserializer = lambda v: json.loads(v.decode('utf-8')),
    enable_auto_commit = False # Kafka commits the offset automatically after a message is read: don’t read it again
)
#To Reset the offset manually, in order to read commited messages
#kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group your-consumer-group --reset-offsets --to-earliest --execute --topic orders
#kafka-consumer-groups.sh --delete --bootstrap-server localhost:9092 --group <your-consumer-group>




while True:
    print("Start Reading the messages fromt the topic")
    message = consumer.poll(timeout_ms = 1000)
    if message:
        for k,v in message.items():
            for msg in v:
                read_data = msg.value
                print(read_data)
                load_data_from_kafka_into_postgres(read_data,cursor,connection)
                read_from_postgres(cursor)
        time.sleep(10)
    else:
        break
        
print("Exiting the Consumer. No more messages to read!")
consumer.close()
cursor.close()
connection.close()    