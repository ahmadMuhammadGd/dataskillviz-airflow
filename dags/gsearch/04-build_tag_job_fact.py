doc = '''
building tags_jobs fact table  
'''

import logging
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago, timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from includes.global_variables.gsearch import (
    POSTGRESQL_CONNECTION_ID,
    SUCCESS_INGESTION_DATASET,
    TAGS_JOBS_FACT_SQL,
    SUCCESS_TAGS_JOBS_FACT_INSERTION_DATASET
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
    dag_id="tags_jobs_fact",
    catchup=False,  
    tags=["fact", "tag", "job"],
    schedule=None,
    default_args=default_args,
    doc_md=doc
)
def build_fact():
    
    task_run_jobs_tags_fact_sql = PostgresOperator(
        task_id='task_run_jobs_tags_fact_sql',
        sql=open(TAGS_JOBS_FACT_SQL, 'r').read(),
        postgres_conn_id=POSTGRESQL_CONNECTION_ID,
        autocommit=True  
    )
    
    @task.branch
    def check_for_new_insertions():        
        sql_check = """
        SELECT 
            max_updated_at
        FROM 
            (
                SELECT 
                    COALESCE(
                        MAX(updated_at)::DATE,
                        DATE '1999-01-01'
                    ) AS max_updated_at
                FROM
                    warehouse.tags_jobs_fact 
            ) subquery
        WHERE 
            max_updated_at = CURRENT_DATE;

        """

        hook = PostgresHook(postgres_conn_id=POSTGRESQL_CONNECTION_ID)
        result = hook.get_first(sql_check)[0]
        if result:
            logging.info('new rows have been inserted, triggering downstream dags')
            return 'update_success'
        else:
            logging.info('no rows have been inserted.')
            
    
    @task(outlets=[SUCCESS_TAGS_JOBS_FACT_INSERTION_DATASET])
    def update_success(**context):
        import datetime
        outlet_events = context["outlet_events"].get(SUCCESS_INGESTION_DATASET, {})
        outlet_events.extra = {"update time": datetime.datetime.now()}
    
    
    task_update_success     = update_success()
    task_check_insertion    = check_for_new_insertions()
    
    task_run_jobs_tags_fact_sql     >> task_check_insertion 
    task_check_insertion            >> task_update_success
    
build_fact()