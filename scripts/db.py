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
        df = prepare_data_for_create_table(df)

        engine = create_engine(get_connection_string())

        # create table
        df.to_sql(name=table_name, con=engine, index=False, if_exists='replace')

        # set id
        with engine.connect() as connection:
            alter_table_sql = f'ALTER TABLE {table_name} ADD PRIMARY KEY (id)'
            connection.execute(alter_table_sql)

        # read the data to see if it was created if needed
        # result_df = pd.read_sql(f'SELECT * FROM {table_name}', con=engine)
        # print(result_df)

    except SQLAlchemyError as e:
        raise RuntimeError('failed to create database table') from e

    except Exception as e:
        raise RuntimeError('unexpected error occurred') from e


def prepare_data_for_create_table(df):

    # adding unique id to the data
    df['id'] = range(1, len(df) + 1)

    # convert arrival date year, month and day of month into a datetime
    df = df.rename(columns={
        'arrival_date_year': 'year',
        'arrival_date_month': 'month',
        'arrival_date_day_of_month': 'day'
    })

    # months are written verbally, convert January to 1, February to 2 etc.
    df['month'] = pd.to_datetime(df['month'], format='%B').dt.month
    # aggregate all values into one column representing arrival date
    df['arrival_date'] = pd.to_datetime(df[['year', 'month', 'day']])
    # drop year month day and week, they are deprecated now
    df.drop(columns={'year', 'month', 'day', 'arrival_date_week_number'}, inplace=True)

    dtype_mapping = {
        'hotel': 'category',
        'is_canceled': 'bool',
        'lead_time': 'int',
        'adults': 'int',
        'children': 'int',
        'babies': 'int',
        'meal': 'category',
        'country': 'category',
        'market_segment': 'category',
        'distribution_channel': 'category',
        'is_repeated_guest': 'bool',
        'previous_cancellations': 'int',
        'previous_bookings_not_canceled': 'int',
        'reserved_room_type': 'category',
        'assigned_room_type': 'category',
        'booking_changes': 'int',
        'deposit_type': 'category',
        'agent': 'int',
        'days_in_waiting_list': 'int',
        'customer_type': 'category',
        'adr': 'float',
        'required_car_parking_spaces': 'int',
        'total_of_special_requests': 'int',
        'reservation_status': 'category',
        'reservation_status_date': 'datetime64[ns]'
    }

    # correct datatype if it is different
    for column, dtype in dtype_mapping.items():
        if df[column].dtype == dtype:
            continue
        if dtype == 'datetime64[ns]':
            df[column] = pd.to_datetime(df[column])
        else:
            df[column] = df[column].astype(dtype)

    return df


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
