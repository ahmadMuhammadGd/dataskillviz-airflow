doc = '''
Process new rows and stage them into the db
'''


from modules.title_seniority_extractor.extractor import Regex_extractor
from modules.tags_extractor.extractor import DictionaryBasedTagExtractor

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

from includes.global_variables.gsearch import (
    POSTGRESQL_CONNECTION_ID,
    SUCCESS_INGESTION_DATASET,
    REVERSE_DICTIONARY_URL,
    JOB_DIM_LOAD_SQL_PATH
)
import pandas as pd 
from psycopg2.extras import execute_values

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}


@dag(
    dag_id="process_load_jobs",
    catchup=False,  
    tags=["gsearch", "staging", "enriching", "dim", "kaggle"],
    schedule=[SUCCESS_INGESTION_DATASET],
    default_args=default_args,
    doc_md=doc
)
def stage():
    
    @task(inlets=[SUCCESS_INGESTION_DATASET])
    def get_file_name(**context):
        dataset_events = context["inlet_events"][SUCCESS_INGESTION_DATASET]
        try:
            file_to_ingest = dataset_events[-1].extra["file_to_ingest"]
        except:
            raise KeyError(f"Ops, `file_to_ingest` in `inlet_events` is empty")
        return file_to_ingest
    
    @task
    def stage_data_to_db(file_path: str):
        
        title_seniority_extractor = Regex_extractor()
        tags_extractor = DictionaryBasedTagExtractor(REVERSE_DICTIONARY_URL)
        def transform(chunk:pd.DataFrame):
            chunk[["cleaned_title", "cleaned_seniority"]] = chunk.apply(
                    lambda row: title_seniority_extractor.extract(
                        row['title'], 
                        row['description']
                    ) , 
                    axis=1, 
                    result_type="expand"
                )
        
            chunk["description_tokens"] = chunk["description"].apply(
                lambda x: tags_extractor.extract(x)
            )
            return chunk
        
        query = """
        INSERT INTO staging.gsearch_jobs (
            title,
            company_name,
            location,
            via,
            description,
            source_job_id,
            date_time,
            cleaned_title,
            cleaned_seniority,
            description_tokens
        )
        VALUES %s
        ON CONFLICT (source_job_id) DO NOTHING;
        """
        
        columns = [
            'title',
            'company_name',
            'location',
            'via',
            'description',
            'job_id',
            'date_time',
            'cleaned_title',
            'cleaned_seniority',
            'description_tokens'
        ]
        chunk_size = 500 # reduce memory usuage

        pg_hook = PostgresHook(postgres_conn_id=POSTGRESQL_CONNECTION_ID)
        conn = pg_hook.get_conn()
        
        with conn.cursor() as cur:
            for chunk in pd.read_csv(file_path, chunksize=chunk_size):
                transformed_chunk = transform(chunk)
                transformed_chunk = transformed_chunk[columns]
                values = [tuple(row) for row in transformed_chunk.itertuples(index=False)]
                execute_values(cur, query, values)
                conn.commit()
                    
        conn.close()

    
    
    task_load_dimension = PostgresOperator(
            task_id='load_dimension',
            postgres_conn_id=POSTGRESQL_CONNECTION_ID,
            sql= open(JOB_DIM_LOAD_SQL_PATH, 'r').read()
        )
        
        
    task_get_file_name      =   get_file_name()
    task_stage_data_to_db   =   stage_data_to_db(task_get_file_name)
    
    task_get_file_name      >> task_stage_data_to_db
    task_stage_data_to_db   >> task_load_dimension
    
    
stage()