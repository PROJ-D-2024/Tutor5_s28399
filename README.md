# Hotel Booking Demand Data Cleaning and Preprocessing

This project focuses on cleaning and preprocessing the Hotel Booking Demand dataset:
https://www.kaggle.com/datasets/jessemostipak/hotel-booking-demand/data. 
The goal is to prepare the data for further analysis by handling missing values, correcting data types, fixing outliers, and normalizing and standardizing relevant columns.

## Prerequisites

To run this project, you need the following installed on your system:

- Python 3.x
- Pandas library
- MySQL database (if you want to create a table in your database and load data from there)

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/sofeeka/Tutor2
   cd hotel-booking-data-cleaning
    ```
2. Install necessary packages
    ```
    pip install pandas kaggle
    ```
    ```
    pip install sqlalchemy
    ```
3. Usage
    Load the dataset. For initial loading use load_dataset from kaggle.py file
    ```
    from kaggle import load_dataset as load_dataset_kaggle
    df = load_dataset_kaggle()
    ```
    
    If you want to save it to db and load it from there later: 
    - configure your user credentials for MySQL database in config.json file. 
    Example config.json file:
        ```
        {
            "database": 
            {
                "user": "your_username",
                "password": "your_password",
                "host": "your_host",
                "port": "your_port",
                "database": "your_database"
            }
        }
        ```
        Change the location of the config file in the db.py method `get_connection_string()` (search `todo` for easier navigation)
        
        When you have a dataframe create a table
        ```
        table_name = 'HotelBookingDemand'
        create_table(df, table_name)
        ```
        
        Now you can load data from the db
        ```
        from db import load_dataset as load_dataset_db
        df = load_dataset_db("HotelBookingDemand")
        ```
    