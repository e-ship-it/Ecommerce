import pandas as pd
from kafka import KafkaConsumer
from dotenv import load_dotenv
import psycopg2, os, json, csv, time, traceback
from pathlib import Path
from kafka.admin import KafkaAdminClient
from psycopg2.extras import execute_batch
from datetime import datetime
cwd_ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def make_output_directory(error_folder):
    directory = cwd_+ "/" + error_folder + "/" 
    if not os.path.exists(directory):
        os.makedirs(directory)

def empty_str_to_null(value):
    if value =="" or value is None:
        return None
    else:
        return value

def load_data_from_kafka_into_postgres(data,cursor,connection):
    values_to_insert = [(d['order_id'],d['product_id'],empty_str_to_null(d['add_to_cart_order']),empty_str_to_null(d['reordered']))
    for d in data]
    print(values_to_insert)
    insert_query = """
    INSERT INTO streams.user_order_activity_stream (order_id,product_id,add_to_cart_order,reordered)
    VALUES (%s, %s, %s, %s)
    """
    try:
        execute_batch(cursor,insert_query,values_to_insert)
        print("Data inserted successfully.")
    except Exception as e:
        print(f"Error inserting data: {e}")
        connection.rollback()
        current_time = datetime.now().strftime("%H%m%d_%H%M%S")
        error_folder = 'user_order_activity_stream_Error_Files'
        make_output_directory(error_folder)
        error_filepath = cwd_ + f'/orders_Stream_Error_Files/error_file_{current_time}.json'
        with open(error_filepath,'w') as f:
            json.dump(values_to_insert,f)
        print(f"Error File created :{error_filepath} ")


def read_from_postgres(cursor):
    cursor.execute("SELECT * FROM streams.user_order_activity_stream")
    print(f"Record Count in table After Insertion: {(len(cursor.fetchall()))}")


try:
    admin_client = KafkaAdminClient()
    print(f" Existing topics: {admin_client.list_topics()}")

    # Step 1: Load environment variables from .env file
    load_dotenv(dotenv_path = cwd_ + '/.env')
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
        "user_order_activity_stream",
        bootstrap_servers = ['localhost:9092'],
        auto_offset_reset='earliest',  # Start from the earliest message (default: 'latest')
        group_id='user_order_activity-group', #mandate
        value_deserializer = lambda v: json.loads(v.decode('utf-8')),
        enable_auto_commit = False # Kafka commits the offset automatically after a message is read: don’t read it again
    )
    #To Reset the offset manually, in order to read commited messages
    #kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group your-consumer-group --reset-offsets --to-earliest --execute --topic orders
    #kafka-consumer-groups.sh --delete --bootstrap-server localhost:9092 --group <your-consumer-group>




    while True:
        print("Start Reading the messages from the topic")
        message = consumer.poll(timeout_ms = 1000)
        if message:
            for k,v in message.items():
                for msg in v:
                    read_data = msg.value
                    print(f"{len(read_data)} records were fetched from the topic")
                    load_data_from_kafka_into_postgres(read_data,cursor,connection)
                    read_from_postgres(cursor)
            time.sleep(10)
        else:
            break

    print("Exiting the Consumer. No more messages to read!")
except Exception:
    traceback.print_exc()
finally:
    cursor.close()
    connection.close()
    #consumer.close() 