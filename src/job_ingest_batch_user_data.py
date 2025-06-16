from dotenv import load_dotenv
import os, csv, traceback
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch

def ingest_data_into_db(data,cursor,connection):

    values_to_insert = [(d['user_id'],d['name'],d['email'],d['birthdate'],d['registration_date'],d['address'],d['phone_number'])
    for d in data]

    insert_query = """
    INSERT INTO batch.users (user_id,name,email,birthdate,registration_date,address,phone_number)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    try:
        execute_batch(cursor,insert_query,values_to_insert)
        print(f"{len(values_to_insert)} records inserted successfully.")
    except Exception as e:
        print(f"Error inserting data: {e}")
        connection.rollback()


def read_csv_file(filePath,cursor,connection):
    for chunk in pd.read_csv(filePath,chunksize = 100):
        ingest_data_into_db(chunk.to_dict('records'),cursor,connection)
    #chunksize parameter - to read a large CSV file in smaller, manageable chunks.

try:
    cwd_ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    print(cwd_)
    load_dotenv(dotenv_path = cwd_ + "/.env")

    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")

    connection = psycopg2.connect(
        dbname = db_name,
        user = db_user,
        password = db_password,
        host = db_host,
        port = db_port
    )

    connection.autocommit = True
    cursor = connection.cursor()

    #read csv File to load into the db
    filePath = cwd_+"/dataset/bittlingmayer/amazonreviews/users.csv"
    print(f"Start reading file : {filePath}")
    read_csv_file(filePath,cursor,connection)
    print(f"End reading file : {filePath}")
    cursor.execute("SELECT COUNT(*) FROM batch.users;")
    print(f"Record count in users table: {cursor.fetchall()}")

except Exception as e:
    traceback.print_exc()
finally:
    cursor.close()
    connection.close()