from dotenv import load_dotenv
load_dotenv(override=True)

import os 

__current_directory = os.path.dirname(os.path.realpath(__file__))

def __construct_full_path(env:str)->str:
    return os.path.join(__current_directory  ,os.getenv(env))

skills_cleaned_json_path    = __construct_full_path(env="tech_tags_json")
dimensions_sql_path         = __construct_full_path(env='dimensions_sql_path')
connection_string           = os.getenv('connection_string')

LANDING_DIR                 =   __construct_full_path(env="LANDING_DIR")
LUKES_DATASET_NAME          =   os.getenv("LUKES_DATASET_NAME")
LUKES_DATASET_REF           =   os.getenv("LUKES_DATASET_REF")
KAGGLE_CONFIG_DIR           =   os.getenv("KAGGLE_CONFIG_DIR")


fasttext_model              =   os.getenv("COMPRESSED_FASTTEXT")