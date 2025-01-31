doc = '''
ingests gsearch CSV file from Kaggle, and identifies new rows for the next steps.
'''

import logging
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
import subprocess
import os
import glob
from includes.global_variables.gsearch import (
    POSTGRESQL_CONNECTION_ID,
    DATASET_REF,
    DOWNLOAD_PATH,
    SUCCESS_INGESTION_DATASET
)
import pandas as pd 
from airflow.models import Variable

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}


@dag(
    dag_id="ingest_gsearch_csv",
    catchup=False,  
    tags=["gsearch", "ingest", "kaggle"],
    schedule=None,
    default_args=default_args,
    doc_md=doc
)
def ingest():
    @task.bash
    def ingest_gsearch_csv()->str:
        kaggle_username = Variable.get('KAGGLE_USERNAME')
        kaggle_key = Variable.get('KAGGLE_KEY')
        
        return f"""
        export KAGGLE_USERNAME={kaggle_username}
        export KAGGLE_KEY={kaggle_key}
        export KAGGLE_CONFIG_DIR=/tmp/kaggle_config
        mkdir -p $KAGGLE_CONFIG_DIR
        kaggle datasets download -d {DATASET_REF} -p {DOWNLOAD_PATH} --unzip
        """

    @task
    def select_csv_file():
        query = os.path.join(DOWNLOAD_PATH, "*csv")
        csv_files = glob.glob(query)
        
        if len(csv_files) == 0:
            raise LookupError(f"Could not find csv file at: {DOWNLOAD_PATH}")
        
        latest_file = max(csv_files, key=os.path.getctime)
        if len(csv_files) > 1:
            logging.info(f"Found `{len(csv_files)}` in the directory `{DOWNLOAD_PATH}`, the last file which is `{latest_file}` will be processed")
        
        return latest_file
        
    @task
    def extract_new_rows(latest_file_path: str):
        df = pd.read_csv(latest_file_path)
        
        pg_hook = PostgresHook.get_hook(POSTGRESQL_CONNECTION_ID)
        max_date_query = """
        SELECT
            COALESCE(
                MAX(date_time), 
                '1900-01-01'
            )::TIMESTAMP AS max_date
        FROM 
            staging.gsearch_jobs
        """
        max_date_df = pg_hook.get_pandas_df(max_date_query)
        max_date_in_staging_table = max_date_df['max_date'].iloc[0]
        
        max_date_in_staging_table = pd.to_datetime(max_date_in_staging_table)
        df['date_time'] = pd.to_datetime(df['date_time'])
        df = df[df['date_time'] > max_date_in_staging_table]
        df_rows_count = len(df.index)
        new_file_name = latest_file_path.replace('.csv', '_NEW_RECORDS.csv')
        df.to_csv(new_file_name, index=False)
        logging.info(f"Filtered data saved to: {new_file_name}")
    
        return new_file_name, df_rows_count
    
    @task(outlets=[SUCCESS_INGESTION_DATASET])
    def update_airflow_dataset(file_to_ingest:str, **context):
        new_file_name, df_rows_count = file_to_ingest
        outlet_events = context["outlet_events"][SUCCESS_INGESTION_DATASET]
        outlet_events.extra = {
            "file_to_ingest": new_file_name, 
            "rows_count": df_rows_count
        }
    
    task_ingest_gsearch_csv     =   ingest_gsearch_csv()
    task_select_csv_file        =   select_csv_file()
    task_extract_new_rows       =   extract_new_rows(task_select_csv_file)
    task_update_airflow_dataset =   update_airflow_dataset(task_extract_new_rows)
    
    task_ingest_gsearch_csv >> task_select_csv_file
    task_select_csv_file    >> task_extract_new_rows
    task_extract_new_rows   >> task_update_airflow_dataset

ingest()