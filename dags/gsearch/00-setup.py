doc = '''
Runs SQL files that initialize the postgre data mart and initialize landing directory
'''

import logging
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago, timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator

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
    task_build_gsearch_data_mart = PostgresOperator(
            task_id='build_gsearch_data_mart',
            postgres_conn_id=POSTGRESQL_CONNECTION_ID,
            sql=open(BUILD_GSEARCH_DATAMART_SQL, 'r').read()
        )

    @task
    def create_gsearch_landing_dir():
        import os 
        exists = os.path.isdir(DOWNLOAD_PATH)
        if not exists:
            os.mkdir(DOWNLOAD_PATH)
            
    task_build_gsearch_data_mart   >>  create_gsearch_landing_dir()
    
build()