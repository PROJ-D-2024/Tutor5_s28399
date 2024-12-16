from kaggle import load_dataset as load_dataset_kaggle
from db import load_dataset as load_dataset_db, prepare_data_for_create_table
from sklearn.model_selection import train_test_split

table_name = 'HotelBookingDemand'

df = load_dataset_kaggle()
# df = load_dataset_db(table_name)


def handle_na(column_name, value):
    df[column_name] = df[column_name].fillna(value)


def drop_column(column_name):
    df.drop(labels=column_name, axis=1, inplace=True)


def handle_duplicates():
    duplicates = df[df.duplicated(keep='first')]
    df.drop_duplicates(inplace=True)


def delete_outliers_and_normalise(column_name):
    fix_outliers(column_name)
    normalize(column_name)


def delete_outliers_and_standardize(column_name):
    fix_outliers(column_name)
    standardize(column_name)


def standardize(column_name):
    mean = df[column_name].mean()
    std = df[column_name].std()

    df[column_name] = (df[column_name] - mean) / std


def normalize(column_name):
    min_val = df[column_name].min()
    max_val = df[column_name].max()

    if min_val != max_val:
        df[column_name] = (df[column_name] - min_val) / (max_val - min_val)


def fix_outliers(column_name, method="iqr"):
    if method.lower() == "iqr":
        Q1 = df[column_name].quantile(0.25)
        Q3 = df[column_name].quantile(0.75)
        IQR = Q3 - Q1

        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
    else:
        mean = df[column_name].mean()
        std_dev = df[column_name].std()
        std_dev_multiplier = 4

        lower_bound = mean - std_dev_multiplier * std_dev
        upper_bound = mean + std_dev_multiplier * std_dev

    df.loc[(df[column_name] < lower_bound), column_name] = lower_bound.astype(df[column_name].dtype)
    df.loc[(df[column_name] > upper_bound), column_name] = upper_bound.astype(df[column_name].dtype)


def get_clean_dataset():
    handle_na('children', 0)
    handle_na('country', 'Unknown')
    handle_na('agent', 0)
    drop_column('company')

    handle_duplicates()

    fix_outliers('lead_time', method='std')
    fix_outliers('stays_in_weekend_nights', method='iqr')
    fix_outliers('stays_in_week_nights', method='iqr')
    fix_outliers('adults', method='std')
    fix_outliers('children', method='std')
    fix_outliers('previous_cancellations', method='std')
    fix_outliers('previous_bookings_not_canceled', method='std')
    fix_outliers('booking_changes', method='std')
    fix_outliers('days_in_waiting_list', method='std')
    fix_outliers('required_car_parking_spaces', method='std')
    fix_outliers('total_of_special_requests', method='std')

    normalize_columns = [
        'lead_time', 'stays_in_weekend_nights', 'stays_in_week_nights',
        'adults', 'children', 'babies', 'previous_cancellations',
        'previous_bookings_not_canceled', 'booking_changes', 'days_in_waiting_list',
        'required_car_parking_spaces', 'total_of_special_requests'
    ]
    standardize_columns = ['adr']

    for col in normalize_columns:
        normalize(col)

    for col in standardize_columns:
        standardize(col)

    final_df = prepare_data_for_create_table(df)

    return final_df


def split_to_xy(data, y_column_index=0):
    df_y = data.iloc[:, y_column_index]
    df_x = data.drop(data.columns[y_column_index], axis=1)

    return df_x, df_y


def get_train_test_xy(data, y_column_index=0):
    train_df, test_df = train_test_split(data, test_size=0.2, random_state=17)

    train_x, train_y = split_to_xy(train_df, y_column_index)
    test_x, test_y = split_to_xy(test_df, y_column_index)

    return train_x, train_y, test_x, test_y
