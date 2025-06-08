from dotenv import load_dotenv
import os, csv, traceback
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch



def ingest_data_into_db(table,columns,data,cursor,connection):
    #input table (list) -> schema.table
    #input columns (list) -> columns in the schema.table
    #input: data (str) -> data to load in schema.table
    # connnection and cursor (str) -> postgres connection objects

    values_to_insert = [tuple(d[col] for col in columns) for d in data]
    column_str = ", ".join(columns)
    insert_placeholder = ", ".join(["%s"]*len(columns))

    insert_query = f"""
    INSERT INTO {table} ({column_str})
    VALUES ({insert_placeholder});
    """
    try:
        execute_batch(cursor,insert_query,values_to_insert)
        print(f"{len(values_to_insert)} records inserted successfully.")
    except Exception as e:
        print(f"Error inserting data: {e}")
        connection.rollback()


def read_csv_file(file,table,connection,cursor):
    #input: file (str) -> csv File name to read 
    #input: table (str) -> schema.table name to Load
    # connnection and cursor (str) -> postgres connection objects

    #read csv File to load into the db
    filePath = cwd_+"/dataset/bittlingmayer/amazonreviews/" + file
    print(f"Start reading file : {filePath}")

    #chunksize parameter - to read a large CSV file in smaller, manageable chunks.    
    for chunk in pd.read_csv(filePath,chunksize = 1000): #pd.read_csv() return a dataframe
        chunk.replace("",pd.NA, inplace = True)
        df = chunk.where(pd.notna(chunk),None)  #where workd element/field wise, Conditional replace, df.where(cond (if true keep this value), else value)
        ingest_data_into_db(table,list(df.columns),df.to_dict('records'),cursor,connection)

    print(f"End reading file : {filePath}")
    cursor.execute(f"SELECT COUNT(*) FROM {table};")
    print(f"Record count in {table}: {cursor.fetchall()}")


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

    read_csv_file("aisles.csv","batch.aisles",connection,cursor)
    read_csv_file("departments.csv","batch.departments",connection,cursor)
    read_csv_file("products.csv","batch.products",connection,cursor)



except Exception as e:
    traceback.print_exc()
finally:
    cursor.close()
    connection.close()