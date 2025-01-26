from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from Tutor6.scripts.prepare_data import get_clean_dataset
from Tutor6.scripts.db import create_table


def download_data(**kwargs):
    # data gets preprocessed inside
    df = get_clean_dataset()
    kwargs['ti'].xcom_push(key='df', value=df)


def upload_data(**kwargs):
    processed_df = kwargs['ti'].xcom_pull(task_ids='df', key='processed_df')
    processed_df = pd.DataFrame(processed_df)
    create_table(processed_df, 'HotelBookingDemand')


default_args = {
    'owner': 'airflow',
}

with DAG(
        dag_id='dag2_data_processing',
        default_args=default_args,
        start_date=datetime(2024, 12, 16),
        catchup=False,
) as dag:
    download_data_task = PythonOperator(
        task_id='download_data_from_sheets',
        python_callable=download_data,
    )

    upload_data_task = PythonOperator(
        task_id='upload_processed_data',
        python_callable=upload_data,
    )

    download_data_task >> upload_data_task
