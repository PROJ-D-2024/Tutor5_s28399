import pandas as pd
import requests
import os


def load_dataset():
    url = 'https://www.kaggle.com/api/v1/datasets/download/jessemostipak/hotel-booking-demand'

    save_to = '../data/'
    date_file_name = 'hotel_bookings.zip'
    zip_file_path = os.path.join(save_to, date_file_name)

    try:
        # make sure folder exists
        os.makedirs(save_to, exist_ok=True)

        # get the zip file from kaggle for initial dataset
        response = requests.get(url)
        response.raise_for_status()

        # save to ../data folder
        with open(zip_file_path, 'wb') as zip_file:
            zip_file.write(response.content)

    except requests.exceptions.ConnectionError as e:
        raise RuntimeError('no internet connection when trying to download the dataset') from e
    except requests.exceptions.RequestException as e:
        raise RuntimeError('web-request related error occurred when trying to download the dataset') from e

    except FileNotFoundError as e:
        raise RuntimeError(f'file {date_file_name} not found error or permission error occurred') from e

    except pd.errors.EmptyDataError as e:
        raise RuntimeError(f'file {date_file_name} is empty') from e

    except Exception as e:
        raise RuntimeError('unexpected error occurred') from e

    return pd.read_csv(zip_file_path)
