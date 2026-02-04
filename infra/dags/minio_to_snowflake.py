#import all the require librries
import os
import boto3
import snowflake.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

#initialize minio connection details
MINIO_ENDPOINT = "http://minio:9000" 
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password123"
BUCKET = "bronze-transactions"
LOCAL_DIR = "/tmp/minio_downloads"  # use absolute path for Airflow

#initialize snowflake connection details
SNOWFLAKE_USER = "SwayamBadhe"
SNOWFLAKE_PASSWORD = "Smvemjsunp@108"
SNOWFLAKE_ACCOUNT = "dp99178.ca-central-1.aws"
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"
SNOWFLAKE_DB = "STOCKS_MDS"
SNOWFLAKE_SCHEMA = "COMMON"

#function to download files from minio
def download_from_minio():
    os.makedirs(LOCAL_DIR, exist_ok=True) #create local directory if not exists
    s3 = boto3.client(                      #quickly connect to minio
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )
    objects = s3.list_objects_v2(Bucket=BUCKET).get("Contents", []) #list all objects in the bucket
    local_files = []
    for obj in objects: #loop through each object
        key = obj["Key"]
        local_file = os.path.join(LOCAL_DIR, os.path.basename(key)) #create local file path
        s3.download_file(BUCKET, key, local_file)
        print(f"Downloaded {key} -> {local_file}") #log the download
        local_files.append(local_file) #append to list
    return local_files

#function to load files to snowflake
def load_to_snowflake(**kwargs):
    local_files = kwargs['ti'].xcom_pull(task_ids='download_minio') #get the list of downloaded files
    if not local_files:
        print("No files to load.")
        return

    conn = snowflake.connector.connect( #connect to snowflake
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DB,
        schema=SNOWFLAKE_SCHEMA
    )
    cur = conn.cursor() #create a cursor object. we use cursor to execute sql commands

    for f in local_files: #loop through each file and upload to snowflake stage
        cur.execute(f"PUT file://{f} @%bronze_stock_quotes_raw") #upload to stage. stage is temporary storage in snowflake. then we copy into table from stage
        print(f"Uploaded {f} to Snowflake stage")

    #copy data from stage to table. table is permanent storage in snowflake and table cant be accessed directly from outside. it has to go via stage.
    cur.execute("""
        COPY INTO bronze_stock_quotes_raw
        FROM @%bronze_stock_quotes_raw
        FILE_FORMAT = (TYPE=JSON)
    """)
    print("COPY INTO executed")

    cur.close() #close the cursor
    conn.close()

#Define default arguments for the DAG.
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 2, 3),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

#Define the DAG. so that we can see minio to snowflake pipeline in airflow ui
with DAG(
    "minio_to_snowflake",
    default_args=default_args,
    schedule_interval="*/1 * * * *",  # every 1 minutes
    catchup=False,
) as dag:

    task1 = PythonOperator(
        task_id="download_minio",
        python_callable=download_from_minio,
    )

    task2 = PythonOperator(
        task_id="load_snowflake",
        python_callable=load_to_snowflake,
        provide_context=True,
    )

    task1 >> task2 #set task dependencies. >> means task1 will run before task2
