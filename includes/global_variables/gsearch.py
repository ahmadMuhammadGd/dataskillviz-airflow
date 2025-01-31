from airflow.datasets import Dataset

POSTGRESQL_CONNECTION_ID    =   "WH_CONN_ID"

DATASET_REF                 =   "lukebarousse/data-analyst-job-postings-google-search"
DOWNLOAD_PATH               =   "/opt/airflow/temp_gsearch"

# Paths
BUILD_GSEARCH_DATAMART_SQL  =   "/opt/airflow/includes/mart/0-wh_ddl.sql"
JOB_DIM_LOAD_SQL_PATH       =   "/opt/airflow/includes/mart/1-jobs_dim.sql"
TAGS_DIM_SQL_PATH           =   "/opt/airflow/includes/mart/2-tags_dim.sql"
TAGS_JOBS_FACT_SQL          =   "/opt/airflow/includes/mart/3-tags_jobs_fact.sql"
TAGS_JOBS_FACT_BACKFILL_SQL =   "/opt/airflow/includes/mart/3-tags_jobs_fact_backfill.sql"
SKILL_REPORT_SQL            =   "/opt/airflow/includes/mart/4-frequency_report.sql"
REDUCED_TAGS_JOBS_FACT_SQL  =   "/opt/airflow/includes/mart/5-reduced_tags_jobs_fact.sql"
FP_GROWTH_REPORT_SQL        =   "/opt/airflow/includes/mart/6-fp_growth_report.sql"
# Links
FASTTEXT_MODEL_URL          =   "https://github.com/ahmadMuhammadGd/skillVector-assets/raw/refs/heads/main/ft_tuned_compress_model_v02.bin"
TAGS_LIST_URL               =   "https://github.com/ahmadMuhammadGd/skillVector-assets/raw/refs/heads/main/limited_tags_list.json"
REVERSE_DICTIONARY_URL      =   "https://github.com/ahmadMuhammadGd/skillVector-assets/raw/refs/heads/main/limited_reverse_dict.json"

# Datasets
SUCCESS_INGESTION_DATASET                   =   Dataset("INFO://gsearch_success_ingestion")
SUCCESS_TAGS_JOBS_FACT_INSERTION_DATASET    =   Dataset("INFO://tags_jobs_fact_insertion")