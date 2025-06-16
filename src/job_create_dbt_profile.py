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
    target_schema1 = os.getenv("TARGET_SCHEMA_1")

    schema_list = {"TARGET_SCHEMA_1":target_schema1}
    target_setup = ["TARGET_SCHEMA_1"]

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
      threads: 2
""")



except Exception:
    traceback.print_exc()