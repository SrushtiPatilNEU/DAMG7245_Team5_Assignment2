from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os
import requests
from zipfile import ZipFile
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
from dotenv import load_dotenv
import snowflake.connector
from bs4 import BeautifulSoup
load_dotenv()
SEC_DATA_URL = "https://www.sec.gov/data-research/sec-markets-data/financial-statement-data-sets"
HEADERS = {
    "User-Agent": "Findata Inc. contact@findata.com",
    "Accept-Encoding": "gzip, deflate",
    "Host": "www.sec.gov"
}
SEC_DATA_DIR = os.path.join(os.getcwd(), "sec_data")
os.makedirs(SEC_DATA_DIR, exist_ok=True)

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

# Initialize S3 Client
s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)

def get_connection():
    return snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )


def get_sec_zip_links():
    response = requests.get(SEC_DATA_URL, headers=HEADERS)
    response.raise_for_status()
    zip_links = [f"https://www.sec.gov{a['href']}" if a['href'].startswith('/') else a['href']
                 for a in BeautifulSoup(response.content, 'html.parser').find_all('a', href=True)
                 if a['href'].endswith('.zip')]
    return zip_links
def download_sec_zip(**kwargs):
    ti = kwargs['ti']
    zip_links = get_sec_zip_links()
    selected_year, selected_quarter = "2024", "q1"
    year_quarter = f"{selected_year}{selected_quarter}"
    zip_url = next((link for link in zip_links if selected_year in link and selected_quarter in link), None)
    if zip_url:
        zip_filename = os.path.join(SEC_DATA_DIR, zip_url.split("/")[-1])
        response = requests.get(zip_url, headers=HEADERS)
        response.raise_for_status()
        with open(zip_filename, 'wb') as file:
            file.write(response.content)
        ti.xcom_push(key='zip_file', value=zip_filename)
        ti.xcom_push(key='year_quarter', value=year_quarter)
def extract_sec_data(**kwargs):
    ti = kwargs['ti']
    zip_file = ti.xcom_pull(task_ids='download_sec_zip', key='zip_file')
    year_quarter = ti.xcom_pull(task_ids='download_sec_zip', key='year_quarter')
    quarter_path = os.path.join(SEC_DATA_DIR, year_quarter)
    os.makedirs(quarter_path, exist_ok=True)
    with ZipFile(zip_file, 'r') as zip_ref:
        zip_ref.extractall(quarter_path)
    extracted_files = [os.path.join(quarter_path, f) for f in ["num.txt", "sub.txt", "pre.txt", "tag.txt"]]
    ti.xcom_push(key='extracted_files', value=extracted_files)
def upload_to_s3(**kwargs):
    ti = kwargs['ti']
    extracted_files = ti.xcom_pull(task_ids='extract_sec_data', key='extracted_files')
    year_quarter = ti.xcom_pull(task_ids='download_sec_zip', key='year_quarter')
    for file in extracted_files:
        s3_client.upload_file(file, S3_BUCKET_NAME, f"sec_data/{year_quarter}/{os.path.basename(file)}")
def load_snowflake(**kwargs):
    conn = get_connection()
    cursor = conn.cursor()
    year_quarter = kwargs['ti'].xcom_pull(task_ids='download_sec_zip', key='year_quarter')
    copy_queries = {
        "sub_table": f"COPY INTO RAW_STAGING.sub_table FROM @sec_data/{year_quarter}/sub.txt FILE_FORMAT = (TYPE='CSV' FIELD_DELIMITER='\t' SKIP_HEADER=1) ON_ERROR='CONTINUE';",
        "num_table": f"COPY INTO RAW_STAGING.num_table FROM @sec_data/{year_quarter}/num.txt FILE_FORMAT = (TYPE='CSV' FIELD_DELIMITER='\t' SKIP_HEADER=1) ON_ERROR='CONTINUE';"
    }
    for query in copy_queries.values():
        cursor.execute(query)
    conn.close()
def verify_snowflake_data(**kwargs):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM RAW_STAGING.sub_table;")
    print(f"Total rows in sub_table: {cursor.fetchone()[0]}")
    conn.close()
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 14),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
dag = DAG(
    'sec_data_dbt_pipeline',
    default_args=default_args,
    description='SEC Data Pipeline with DBT Transformations',
    schedule_interval=None,
    catchup=False
)
scrape_task = PythonOperator(task_id='get_sec_zip_links', python_callable=get_sec_zip_links, dag=dag)
download_task = PythonOperator(task_id='download_sec_zip', python_callable=download_sec_zip, dag=dag)
extract_task = PythonOperator(task_id='extract_sec_data', python_callable=extract_sec_data, dag=dag)
upload_task = PythonOperator(task_id='upload_to_s3', python_callable=upload_to_s3, dag=dag)
load_task = PythonOperator(task_id='load_to_snowflake', python_callable=load_snowflake, dag=dag)
verify_task = PythonOperator(task_id='verify_snowflake_data', python_callable=verify_snowflake_data, dag=dag)
dbt_run_task = BashOperator(
    task_id="run_dbt_transformations",
    bash_command="dbt run --profiles-dir /opt/airflow/dbt_project/",
    dag=dag,
)
scrape_task >> download_task >> extract_task >> upload_task >> load_task >> verify_task >> dbt_run_task
