from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from wow_api_extractor import get_access_token, get_races, upload_to_gcs
import os
import dotenv

dotenv.load_dotenv()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'wow_data_extraction',
    default_args=default_args,
    description='WoW Data Extraction Pipeline',
    schedule_interval=timedelta(days=1),
)

def extract_and_store_races(**kwargs):
    client_id = os.getenv('CLIENT_ID')
    client_secret = os.getenv('CLIENT_SECRET')
    access_token = get_access_token(client_id, client_secret)
    races = get_races(access_token)
    upload_to_gcs('your-bucket-name', 'wow_data/races.json', races)

extract_races = PythonOperator(
    task_id='extract_and_store_races',
    python_callable=extract_and_store_races,
    dag=dag,
)
