doc = '''
load tags from `TAGS_LIST_URL`, get word embedding, and load them to tags dim 
'''


from modules.title_seniority_extractor.extractor import Regex_extractor
from modules.vectorizer.fasttext import CustomFasttext, SemanticPreprocessor


import logging
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
import subprocess
import os
import glob
from includes.global_variables.gsearch import (
    POSTGRESQL_CONNECTION_ID,
    TAGS_LIST_URL,
    FASTTEXT_MODEL_URL,
    TAGS_DIM_SQL_PATH
)
import pandas as pd 
from psycopg2.extras import execute_values
import json 
import requests


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}


@dag(
    dag_id="process_tags",
    catchup=False,  
    tags=["gsearch", "tags", "vectors", "dim"],
    schedule=None,
    default_args=default_args,
    doc_md=doc
)
def load():
    @task
    def ingest_tags_list() -> list[str]|None:
        tags_list_request = requests.get(TAGS_LIST_URL)
        if tags_list_request.status_code == 200:
            return tags_list_request.json()
        else:
            raise Exception(f"An error occured while requesting `tags list`. Error code: {tags_list_request.status_code}")
   
    @task
    def load_dimension(tags_list:list[str]):
        vectorizer = CustomFasttext(model_path_or_link=FASTTEXT_MODEL_URL)
        
        batch_size = 500 #reduce memory usage
        pg_hook = PostgresHook(postgres_conn_id=POSTGRESQL_CONNECTION_ID)
        conn = pg_hook.get_conn()
        
        with open(TAGS_DIM_SQL_PATH, 'r') as f:
            sql_transformation = f.read()
        
        with conn.cursor() as cur:
            for batch_start in range(0, len(tags_list), batch_size):
                rows = [(tag, vectorizer.get_vector(tag)) for tag in tags_list[batch_start:batch_start + batch_size]]    
                execute_values(cur, sql_transformation, rows)
            conn.commit()
        conn.close()
        
    task_ingest_tags_list   =   ingest_tags_list()
    task_load_dimension     =   load_dimension(task_ingest_tags_list)
    
    
    task_ingest_tags_list   >>  task_load_dimension
load()