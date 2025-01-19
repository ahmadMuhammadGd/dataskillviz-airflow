doc = '''
create tool report table 
'''

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago, timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator

from includes.global_variables.gsearch import (
    POSTGRESQL_CONNECTION_ID,
    SKILL_REPORT_SQL,
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
    dag_id="tool_report",
    catchup=False,  
    tags=["gsearch", "gold", "aggregation", "tool"],
    schedule=[SUCCESS_TAGS_JOBS_FACT_INSERTION_DATASET],
    default_args=default_args,
    doc_md=doc
)
def build_report():
    
    task_run_skill_report_sql = PostgresOperator(
        task_id='run_skill_report_sql',
        sql=open(SKILL_REPORT_SQL, 'r').read(),
        postgres_conn_id=POSTGRESQL_CONNECTION_ID,
        autocommit=True  
    )
    task_run_skill_report_sql

build_report()