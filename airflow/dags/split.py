from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from Tutor2.scripts.prepare_data import get_clean_dataset, get_train_test_xy


def authenticate_gspread(json_keyfile_path):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(json_keyfile_path, scope)
    client = gspread.authorize(creds)
    return client


def upload_to_google_sheets(df, sheet_name, json_keyfile_path):
    client = authenticate_gspread(json_keyfile_path)

    try:
        sheet = client.open(sheet_name)
    except gspread.SpreadsheetNotFound:
        sheet = client.create(sheet_name)

    worksheet = sheet.sheet1
    worksheet.clear()
    worksheet.update([df.columns.values.tolist()] + df.values.tolist())


def download_data(**kwargs):
    df = get_clean_dataset()
    kwargs['ti'].xcom_push(key='dataset', value=df)


def split_and_upload_data(**kwargs):
    df = pd.DataFrame(kwargs['ti'].xcom_pull(task_ids='download_data', key='dataset'))

    train_X, test_X, train_y, test_y = get_train_test_xy(df)

    json_keyfile_path = '/config/config.json'
    upload_to_google_sheets(pd.concat([train_X, train_y], axis=1), 'Training Dataset', json_keyfile_path)
    upload_to_google_sheets(pd.concat([test_X, test_y], axis=1), 'Testing Dataset', json_keyfile_path)


# Define DAG
with DAG(
        dag_id="dag1_download_split",
        start_date=datetime(2024, 12, 16),
        schedule_interval="@once",
        catchup=False
) as dag:
    task_download_data = PythonOperator(
        task_id="download_data",
        python_callable=download_data
    )

    task_split_upload = PythonOperator(
        task_id="split_and_upload_data",
        python_callable=split_and_upload_data
    )

    task_download_data >> task_split_upload
