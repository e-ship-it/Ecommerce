import pytest
import psycopg2
from dotenv import load_dotenv
import os

@pytest.fixture
def db_connection():
    # Step 1: Load environment variables from .env file
    cwd_ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    load_dotenv(dotenv_path = cwd_ + '/.env')
    # Step 2: Get the connection parameters from the environment
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")

    # connect to postgres DB
    conn = psycopg2.connect(
        dbname = db_name,
        user = db_user,
        password = db_password,
        host = db_host,
        port = db_port
    )
    yield conn
    conn.close()

@pytest.fixture
def temp_table(db_connection):
    cur = db_connection.cursor()
    # Create temporary test table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS staging.test_temp_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50),
            age INT
        );
    """)
    db_connection.commit()

    yield "staging.test_temp_table"  # yield the table name for use in tests

    # Drop the test table after test finishes
    cur.execute("DROP TABLE IF EXISTS staging.test_temp_table;")
    db_connection.commit()

def test_insert_and_query(db_connection, temp_table):
    cur = db_connection.cursor()

    # Insert sample data
    cur.execute("""
        INSERT INTO {} (name, age) VALUES
        ('Alice', 25),
        ('Bob', 30);
    """.format(temp_table))
    db_connection.commit()

    # Query back
    cur.execute("SELECT name, age FROM {} ORDER BY name".format(temp_table))
    rows = cur.fetchall()

    assert rows == [('Alice', 25), ('Bob', 30)], f"Expected data not found. Got: {rows}"
