from prefect import flow, task
import subprocess
import generate_logFile
import os
from dotenv import load_dotenv

project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) # Global variable

@task
def run_job_simulate_kafka_orders_streaming(logger):
    cwd = str(project_dir) + "/src" 
    logger.info("Running job_simulate_kafka_orders_streaming...")
    result = subprocess.run(["python", "job_simulate_kafka_orders_streaming.py"], capture_output=True, text=True,cwd=cwd)
    logger.info(result.stdout)
    if result.returncode != 0:
        logger.error(result.stderr)
        raise Exception("job_simulate_kafka_orders_streaming: failed!!")

@task
def run_job_ingest_kafka_streaming_orders_data(logger):
    cwd = str(project_dir) + "/src" 
    print(cwd)
    logger.info("Running job_ingest_kafka_streaming_orders_data...")
    result = subprocess.run(["python", "job_ingest_kafka_streaming_orders_data.py"], capture_output=True, text=True,cwd=cwd)
    logger.info(result.stdout)
    if result.returncode != 0:
        logger.error(result.stderr)
        raise Exception("job_ingest_kafka_streaming_orders_data: failed!!")

@task
def run_job_simulate_kafka_user_order_activity_stream(logger):
    cwd = str(project_dir) + "/src" 
    logger.info("Running job_simulate_kafka_user_order_activity_stream...")
    result = subprocess.run(["python", "job_simulate_kafka_user_order_activity_stream.py"], capture_output=True, text=True,cwd=cwd)
    logger.info(result.stdout)
    if result.returncode != 0:
        logger.error(result.stderr)
        raise Exception("job_simulate_kafka_user_order_activity_stream: failed!!")

@task
def run_job_ingest_kafka_streaming_user_order_activity(logger):
    cwd = str(project_dir) + "/src" 
    print(cwd)
    logger.info("Running job_ingest_kafka_streaming_user_order_activity...")
    result = subprocess.run(["python", "job_ingest_kafka_streaming_user_order_activity.py"], capture_output=True, text=True,cwd=cwd)
    logger.info(result.stdout)
    if result.returncode != 0:
        logger.error(result.stderr)
        raise Exception("job_ingest_kafka_streaming_user_order_activity: failed!!")        


@task
def run_dbt(logger,dbt_path):
    logger.info("Running dbt...")
    cwd = os.path.join(project_dir, "dbt_pipeline", "dbt_pipeline")
    result = subprocess.run([str(dbt_path), "run"], capture_output=True, text=True, cwd = cwd,shell=True)
    logger.info(result.stdout)
    if result.returncode != 0:
        logger.error(result.stderr)
        raise Exception("dbt run failed")

@flow
def my_pipeline():
    # start kafka severs first

        
    #Load environment variables from .env file
    load_dotenv(dotenv_path = project_dir + '/.env')
    #Get the connection parameters from the environment
    dbt_path = os.getenv("DBT_PATH")
    print(dbt_path)

    logger = generate_logFile.setup_run_logger()

    for _ in range(5):
        run_job_simulate_kafka_orders_streaming(logger)
        run_job_ingest_kafka_streaming_orders_data(logger)
        run_job_simulate_kafka_user_order_activity_stream(logger)
        run_job_ingest_kafka_streaming_user_order_activity(logger)
    run_dbt(logger,dbt_path)
    logger.info("Job finished.")
    for handler in logger.handlers:
        handler.flush()
        handler.close()

    #run_dbt(logger)

if __name__ == "__main__":
    my_pipeline()
