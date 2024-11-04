import os.path

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import create_engine
import pandas as pd
import json


def get_connection_string():
    # todo change path to your config.json file
    config_file_path = '../config/config.json'

    try:
        if not os.path.exists(config_file_path):
            raise FileNotFoundError

        with open(config_file_path) as config_file:
            config = json.load(config_file)

        try:
            user = config['database']['user']
            password = config['database']['password']
            host = config['database']['host']
            port = config['database']['port']
            database = config['database']['database']
        except KeyError as e:
            raise KeyError('could not find needed key in config file') from e

        # I use Filess https://filess.io/ and MySQL
        connection_string = f'mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}'
        return connection_string

    except FileNotFoundError as e:
        raise RuntimeError(f'configuration file {config_file_path} not found error or permission error') from e

    except json.JSONDecodeError as e:
        raise RuntimeError('error parsing the configuration file') from e

    except Exception as e:
        raise RuntimeError('unexpected error occurred') from e


def create_table(df, table_name):
    try:
        engine = create_engine(get_connection_string())

        # create table
        df.to_sql(name=table_name, con=engine, index=False, if_exists='replace')

        # read the data to see if it was created if needed
        # result_df = pd.read_sql(f'SELECT * FROM {table_name}', con=engine)
        # print(result_df)

    except SQLAlchemyError as e:
        raise RuntimeError('failed to create database table') from e

    except Exception as e:
        raise RuntimeError('unexpected error occurred') from e


def load_dataset(table_name):
    try:
        connection_string = get_connection_string()
        engine = create_engine(connection_string)

        # return dataframe
        return pd.read_sql(f'SELECT * FROM {table_name}', con=engine)

    except SQLAlchemyError as e:
        raise RuntimeError('failed to load dataset') from e

    except Exception as e:
        raise RuntimeError('unexpected error occurred') from e
