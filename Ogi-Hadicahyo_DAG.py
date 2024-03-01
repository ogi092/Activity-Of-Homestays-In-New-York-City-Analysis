'''
=================================================
Name  : Ogi Hadicahyo

This program was created to automate the transformation and load of data from PostgreSQL to ElasticSearch. The dataset used is a dataset containing Airbnb open data from New York City
=================================================
'''

# Import Libraries
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
from sqlalchemy import create_engine # Connection to PostgreSQL
import pandas as pd
import numpy as np

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

# Function to load CSV data into PostgreSQL
def load_csv_to_postgres():
    """
    Function to load data from a CSV file into PostgreSQL.
    """
    database = "airflow_m3"
    username = "airflow_m3"
    password = "airflow_m3"
    host = "postgres"

    # Create a PostgreSQL connection URL
    postgres_url = f"postgresql+psycopg2://{username}:{password}@{host}/{database}"

    # Use this URL when creating a SQLAlchemy connection
    engine = create_engine(postgres_url)
    conn = engine.connect()

    # Read data from CSV file
    data = pd.read_csv('/opt/airflow/dags/P2M3_Ogi-Hadicahyo_data_raw.csv')

    # Write data to PostgreSQL table 'table_m3' with replace mode
    data.to_sql('table_m3', conn, index=False, if_exists='replace')

# Function to fetch data from PostgreSQL and save as CSV
def fetch_data():
    """
    Function to fetch data from PostgreSQL and save as CSV.
    """
    database = "airflow_m3"
    username = "airflow_m3"
    password = "airflow_m3"
    host = "postgres"

    # Create a PostgreSQL connection URL
    postgres_url = f"postgresql+psycopg2://{username}:{password}@{host}/{database}"

    # Use this URL when creating a SQLAlchemy connection
    engine = create_engine(postgres_url)
    conn = engine.connect()

    # Read data from PostgreSQL table 'table_m3'
    data = pd.read_sql_query("select * from table_m3", conn)

    # Save data as CSV file
    data.to_csv('/opt/airflow/dags/P2M3_Ogi-Hadicahyo_data_new.csv', sep=',', index=False)

# Function for data preprocessing
def preprocessing():
    """
    Function for data preprocessing.
    """
    # Read data from CSV file
    data = pd.read_csv('/opt/airflow/dags/P2M3_Ogi-Hadicahyo_data_new.csv')
    
    # Change column names to lowercase and replace spaces with underscores
    data.columns = [col.lower().replace(" ", "_") for col in data.columns]

    # Drop unnecessary columns
    data.drop(columns=["id", "house_rules", "license", "country_code", "country"], axis=1, inplace=True)

    # Drop rows with missing values
    data.dropna(inplace=True)

    # Drop duplicate rows
    data.drop_duplicates(inplace=True)

    # Remove dollar sign and comma from 'price' column
    data["price"] = data["price"].apply(lambda x: remove_dollar_sign(x))

    # Remove dollar sign and comma from 'service_fee' column
    data["service_fee"] = data["service_fee"].apply(lambda x: remove_dollar_sign(x))

    # Convert 'last_review' column to datetime format
    data["last_review"] = pd.to_datetime(data["last_review"])

    # Save cleaned data to new CSV file
    data.to_csv('/opt/airflow/dags/P2M3_Ogi-Hadicahyo_data_clean.csv', index=False)

# Function to upload data to Elasticsearch
def upload_to_elasticsearch():
    """
    Function to upload data to Elasticsearch.
    """
    # Establish connection to Elasticsearch
    es = Elasticsearch("http://elasticsearch:9200")

    # Read cleaned data from CSV file
    data = pd.read_csv('/opt/airflow/dags/P2M3_Ogi-Hadicahyo_data_clean.csv')

    # Iterate over each row and index it in Elasticsearch
    for i, r in data.iterrows():
        doc = r.to_dict() # Convert the row to a dictionary
        res = es.index(index="table_m3", id=i+1, body=doc)
        print(f"Response from Elasticsearch: {res}")

# Default arguments for DAG
default_args = {
    'owner': 'Ogi',
    'start_date': datetime(2024, 2, 23, 15, 00) - timedelta(hours=7)
}

# Define DAG
with DAG(
    "P2M3_Ogi-Hadicahyo_DAG_hck", 
    description='Milestone_3',
    schedule_interval='30 6 * * *', # Set schedule to run airflow at 06:30.
    default_args=default_args,
    catchup=False
) as dag:
    # Task 1: Load CSV data to PostgreSQL
    load_csv_task = PythonOperator(
        task_id='load_csv_to_postgres',
        python_callable=load_csv_to_postgres)

    # Task 2: Fetch data from PostgreSQL
    fetch_data_pg = PythonOperator(
        task_id='fetch_postgres_data',
        python_callable=fetch_data)

    # Task 3: Perform data preprocessing
    edit_data = PythonOperator(
        task_id='edit_data',
        python_callable=preprocessing)

    # Task 4: Upload data to Elasticsearch
    upload_data = PythonOperator(
        task_id='upload_data_elastic',
        python_callable=upload_to_elasticsearch)

    # Define task dependencies
    load_csv_task >> fetch_data_pg >> edit_data >> upload_data