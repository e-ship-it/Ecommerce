import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
# Replace with your actual credentials

try:
    # Step 1: Load environment variables from .env file
    load_dotenv(dotenv_path = cwd_ + '/.env')
    # Step 2: Get the connection parameters from the environment
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")

# Example: Read a dbt model
df = pd.read_sql("SELECT * FROM opts_hub.dim_user LIMIT 100", engine)
df.head(10)
