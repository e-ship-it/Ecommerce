import pandas as pd
from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

chunk_size = 100  # number of rows per batch
file_path = '/data/retailrocket/retailrocket_event.csv'

for chunk in pd.read_csv(file_path, chunksize=chunk_size):
    for _, row in chunk.iterrows():
        event = row.to_dict()
        producer.send('retailrocket_events', value=event)
    producer.flush()
    print(f'Sent batch of {chunk_size} events.')
    time.sleep(60)  # wait 1 minute before next batch

import pandas as pd
from sqlalchemy import create_engine

# Load CSV
df = pd.read_csv('/data/retailrocket/events.csv')

# Connect to PostgreSQL
engine = create_engine('postgresql://user:password@localhost:5432/dbname')

# Load data into PostgreSQL table
df.to_sql('events', engine, if_exists='replace', index=False)
