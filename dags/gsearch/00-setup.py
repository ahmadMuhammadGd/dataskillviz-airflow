doc = '''
Runs SQL files that initialize the postgre data mart and initialize landing directory
'''

import logging
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook

from includes.global_variables.gsearch import (
    POSTGRESQL_CONNECTION_ID,
    BUILD_GSEARCH_DATAMART_SQL,
    DOWNLOAD_PATH
)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

@dag(
    dag_id="build_gsearch",
    catchup=False,  
    tags=["gsearch", "mart-init"],
    schedule=None,
    default_args=default_args,
    doc_md=doc
)
def build():
    @task
    def build_gsearch_data_mart():
        pg_hook = PostgresHook.get_hook(POSTGRESQL_CONNECTION_ID)
        pg_hook.run(BUILD_GSEARCH_DATAMART_SQL)

    @task
    def create_gsearch_landing_dir():
        import os 
        exists = os.path.isdir(DOWNLOAD_PATH)
        if not exists:
            os.mkdir(DOWNLOAD_PATH)
            
    build_gsearch_data_mart()   >>  create_gsearch_landing_dir()
    
build()