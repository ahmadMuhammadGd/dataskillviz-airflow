doc = """
### Tool Frequency Pattern Growth Report

This DAG identifies frequent patterns in `tags_list` from `reduced_tags_jobs_fact` 
using the FP-Growth algorithm. The results are transformed into a network graph 
and stored in `warehouse.tags_fp_growth`.

#### Steps:
1. Fetch data from `warehouse.reduced_tags_jobs_fact`.
2. Run FP-Growth to identify frequent patterns.
3. Transform patterns into edge structure for network graphs.
4. Load results into `warehouse.tags_fp_growth`.

**Schedule**: Triggered after successful insertion into `tags_jobs_fact`.
"""


from psycopg2.extras import execute_values
import pandas as pd 
import logging
from mlxtend.frequent_patterns import fpgrowth
from mlxtend.preprocessing import TransactionEncoder

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

from includes.global_variables.gsearch import (
    POSTGRESQL_CONNECTION_ID,
    FP_GROWTH_REPORT_SQL,
    SUCCESS_TAGS_JOBS_FACT_INSERTION_DATASET
)
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago, timedelta

from typing import List, Any
from itertools import combinations


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}


@dag(
    dag_id="tool_frequency_pattern_growth_report",
    catchup=False,  
    tags=["gold", "aggregation", "skill", "FP"],
    schedule=[SUCCESS_TAGS_JOBS_FACT_INSERTION_DATASET],
    default_args=default_args,
    doc_md=doc
)
def build_report():
    @task
    def fetch_upstream_data() -> list[list[int]]:
        '''
        read tags_list from tags_jobs_reduced table
        '''
        pg_hook = PostgresHook.get_hook(POSTGRESQL_CONNECTION_ID)
        conn = pg_hook.get_conn()
        sql = """
            SELECT 
                tags_list
            FROM 
                warehouse.reduced_tags_jobs_fact
        """
        with conn.cursor() as cur:
            cur.execute(sql)
            result = cur.fetchall()
        # result comes in form of list of tuples, each tuple represents a row
        result = [row[0] for row in result]
        logging.info(f"feched data sample: {result[:3]} ..")
        return result
        
    @task
    def calculate_fp_growth(itemset:list[list[Any]]) -> pd.DataFrame:
        n_transactions = len(itemset)
        te = TransactionEncoder()
        te_ary = te.fit(itemset).transform(itemset)
        df = pd.DataFrame(te_ary, columns=te.columns_)
        fp_res = fpgrowth(df, min_support=0.005, use_colnames=True)
        fp_res_filtered = fp_res[fp_res['itemsets'].apply(lambda x: len(x) >= 2)]
        return fp_res


    @task
    def transform_fp_growth_to_graph(fp_dataframe:pd.DataFrame):
        edges = []
        for _, row in fp_dataframe.iterrows():
            support = row["support"]
            itemset = row["itemsets"]
            for pair in combinations(itemset, 2):
                edges.append((pair[0], pair[1], support))
        res = pd.DataFrame(edges, columns=["source", "target", "support"])
        logging.info(f"fp growth source, target, support list: {edges}")
        return res
    
    
    task_truncate_fp_growth = PostgresOperator(
        task_id='truncate_fp_growth',
        sql="TRUNCATE TABLE warehouse.tags_fp_growth;",
        postgres_conn_id=POSTGRESQL_CONNECTION_ID,
        autocommit=True  
    )

    @task
    def load_fp_growth_report(edges:pd.DataFrame):
        grouped_edges=edges.groupby(["source", "target"]).mean().reset_index()

        def load_batch(cur, stmt, chunk:pd.DataFrame):
            values = [tuple(row) for row in chunk.itertuples(index=False)]
            execute_values(cur, stmt, values)
        
        pg_hook = PostgresHook(postgres_conn_id=POSTGRESQL_CONNECTION_ID)
        
        chunk_size = 1000
        chunks = [grouped_edges.iloc[i:i + chunk_size] for i in range(0, len(grouped_edges), chunk_size)]

        stmt = None
        with open(FP_GROWTH_REPORT_SQL, 'r') as f:
            stmt = f.read()
        
        if not stmt or len(stmt) == 0:
            raise Exception(f"fp growth SQL can't be `empty string` or `None`")
        
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                for chunk in chunks:
                    load_batch(cur, stmt, chunk)
            conn.commit()



    task_get_upstream_data              = fetch_upstream_data()
    task_calculate_fp_growth            = calculate_fp_growth(task_get_upstream_data)
    task_transform_fp_growth_to_graph   = transform_fp_growth_to_graph(task_calculate_fp_growth)
    task_load_fp_growth_report          = load_fp_growth_report(task_transform_fp_growth_to_graph)
    

    task_get_upstream_data              >>      task_calculate_fp_growth
    task_calculate_fp_growth            >>      task_transform_fp_growth_to_graph
    task_transform_fp_growth_to_graph   >>      task_truncate_fp_growth
    task_truncate_fp_growth             >>      `task_load_fp_growth_report`
build_report()