import os, traceback
from dotenv import load_dotenv
from pathlib import Path

def check_directory(dir):
    if not os.path.exists(dir):
        os.makedirs(dir)

try:
    #Load environment variables from .env file
    load_dotenv()

    profile_name = "DBT_PROFILE"
    dbname = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    schema_batch = os.getenv("SCHEMA_BATCH")
    schema_stream = os.getenv("SCHEMA_STREAM")

    schema_list = {"DEV_SCHEMA1":schema_batch,"DEV_SCHEMA2":schema_stream}
    target_setup = ["DEV_SCHEMA1","DEV_SCHEMA2"]

    dbt_profile_dir = str(Path.home()) + "/.dbt/"
    check_directory(dbt_profile_dir)
# Target: label you assign to a specific output. Exact environment or setup NAME you want dbt to work with.
# Output: connection settings for different environments or schemas.
#the target (dev) needs to match one of the output names exactly in the outputs section.
# use double space instead of tab to indent the code in yml file. YAML is not a mark up language.
    with open(dbt_profile_dir+"profiles.yml",'w') as f:
        f.write(f"""{profile_name}:
  target: {target_setup[0]}
  outputs:""")
  
        for target in target_setup:
            f.write(f"""
    {target}:
      type: postgres
      host: {host}
      user: {user}
      password: {password}
      port: {port}
      dbname: {dbname}
      schema: {schema_list[target]}
""")



except Exception:
    traceback.print_exc()