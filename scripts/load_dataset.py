from sqlalchemy import create_engine
import pandas as pd
import requests
import json
import os


def load_from_csv():
    url = 'https://www.kaggle.com/api/v1/datasets/download/jessemostipak/hotel-booking-demand'

    save_to = '../data/'
    date_file_name = 'hotel_bookings.zip'
    zip_file_path = os.path.join(save_to, date_file_name)

    # make sure folder exists
    os.makedirs(save_to, exist_ok=True)

    # get the zip file from kaggle for initial dataset
    response = requests.get(url)
    response.raise_for_status()

    # save to ../data folder
    with open(zip_file_path, 'wb') as zip_file:
        zip_file.write(response.content)

    return pd.read_csv(zip_file_path)


def load_from_database(table_name):
    # todo change path to your config.json file
    with open('../config/config.json') as config_file:
        config = json.load(config_file)

    user = config['database']['user']
    password = config['database']['password']
    host = config['database']['host']
    port = config['database']['port']
    database = config['database']['database']

    # I use Filess https://filess.io/ and MySQL
    connection_string = f'mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}'
    engine = create_engine(connection_string)

    # read the data to see if it was created if needed
    df = pd.read_sql(f'SELECT * FROM {table_name}', con=engine)
