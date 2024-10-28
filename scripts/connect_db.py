import pandas as pd
from sqlalchemy import create_engine
import json




def create_table(df, table_name):
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

    print(df)

    # create table
    df.to_sql(name=table_name, con=engine, index=False, if_exists='replace')

    # read the data to see if it was created if needed
    result_df = pd.read_sql(f'SELECT * FROM {table_name}', con=engine)
    print(result_df)


table_name = 'HotelBookingDemand'
df = load_from_csv()
create_table(df, table_name)
