from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from Tutor2.scripts.prepare_data import get_clean_dataset


def download_data(**kwargs):
    # data gets preprocessed inside
    df = get_clean_dataset()
    kwargs['ti'].xcom_push(key='dataset', value=df)


def upload_data(**kwargs):
    processed_df = kwargs['ti'].xcom_pull(task_ids='dataset', key='processed_dataset')
    processed_df = pd.DataFrame(processed_df)


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dag2_data_processing',
    default_args=default_args,
    description='Clean and process data DAG',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    download_data_task = PythonOperator(
        task_id='download_data_from_sheets',
        python_callable=download_data,
        provide_context=True,
    )

    upload_processed_data_task = PythonOperator(
        task_id='upload_processed_data',
        python_callable=upload_data,
        provide_context=True,
    )

    download_data_task >> upload_data
