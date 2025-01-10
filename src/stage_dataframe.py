from modules.title_seniority_extractor.extractor import Regex_extractor
from modules.vectorizer.fasttext import CustomFasttext, SemanticPreprocessor

import os 
import pandas as pd
from global_variables import LANDING_DIR
from glob import glob
from time import time

import psycopg2
from psycopg2.extras import execute_values
from tqdm import tqdm

from global_variables import (
    connection_string,
    fasttext_model
)

import json 

def get_csv_file(laning_path:str)->pd.DataFrame:
    files = glob(f"{laning_path}/*.csv")
    last_landing_file = max(files, key=os.path.getctime)
    
    return pd.read_csv(last_landing_file), last_landing_file


def process_csv(df: pd.DataFrame) -> pd.DataFrame:
    tqdm.pandas()
    print("preparing tokenizer and extractor")
    tokenizer = (lambda x: sementic_processor.preprocess(x).split())
    title_seniority_extractor = extractor.extract
    
    print("droping null")
    df_filtered = df.dropna(subset=['title', 'description'])
    print("droping duplicates")
    df_filtered = df_filtered.drop_duplicates(subset=["job_id"])
    enriched = df_filtered.copy()
    
    print("enriching title and seniority")
    enriched[["cleaned_title", "cleaned_seniority"]] = enriched.apply(
        lambda row: title_seniority_extractor(row['title'], row['description']), axis=1, result_type="expand"
    )

    print("desciption tokenization ..")
    enriched["description_tokens"] = enriched["description"].apply(lambda x: tokenizer(x))
    
    return enriched

def save_temp(df:pd.DataFrame) -> None:
    df.to_csv(
        path_or_buf= os.path.join(LANDING_DIR, f"TEMP_{int(time())}.csv")
        ,index=False
    )

def clear_temp() -> None:
    temp_files = glob(f"{LANDING_DIR}/TEMP_*csv")
    for temp in temp_files:
        os.remove(temp)

def is_temp(path:str) -> bool:
    return "TEMP_" not in path

def stage(df: pd.DataFrame, connection_string: str) -> bool:
    query = """
    INSERT INTO staging.gsearch_jobs (
        title
        , company_name
        , location
        , via
        , description
        , source_job_id
        , date_time
        , cleaned_title
        , cleaned_seniority
        , description_tokens
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
    df = df[columns]
    import ast
    df['description_tokens'] = df['description_tokens'].apply(lambda x: ast.literal_eval(x) if pd.notna(x) else x) #quick fix
    chunk_size = 500
    try:
        with psycopg2.connect(connection_string) as conn:
            with conn.cursor() as cur:
                for start in tqdm(range(0, len(df), chunk_size), desc="loading chunks to database", total=len(df)//chunk_size):
                    chunk = df.iloc[start:start + chunk_size]
                    values = [tuple(x) for x in chunk.values]
                    
                    execute_values(cur, query, values)
                    conn.commit()
                    
                print(f"✅ Successfully inserted data into the staging table in chunks of {chunk_size}.")
                return True
    except Exception as e:
        raise Exception (f"Error occurred: {e}")
    
    
if __name__ == "__main__":
    print("🔄 Initializing extractor, fasttext model, and sementic processor...")
    extractor = Regex_extractor()
    vectorizer = CustomFasttext(fasttext_model)
    raw_bi_terms = "https://raw.githubusercontent.com/ahmadMuhammadGd/skillVector-assets/refs/heads/main/lookups/bi_terms.json"
    raw_aliases = "https://raw.githubusercontent.com/ahmadMuhammadGd/skillVector-assets/refs/heads/main/lookups/aliases.json"
    sementic_processor = SemanticPreprocessor(bigrams=raw_bi_terms, aliases=raw_aliases)

    print("🔄 Fetching the latest CSV file...")
    df, path = get_csv_file(LANDING_DIR)  # get latest file whether it's new or temp
    print(f"Fetched: {path}")
    if is_temp(path):
        print("📝 Processing and enriching the data...")
        enriched_df = process_csv(df)
        print("💾 Saving processed data to temporary CSV...")
        save_temp(enriched_df)
    else:
        print("🔄 Using the existing temporary file...")
        enriched_df = df

    print("🚀 Staging data to the database...")
    if stage(enriched_df, connection_string):
        print("✅ Data successfully staged! Clearing temporary files...")
        clear_temp()
    else:
        print("❌ Error occurred while staging data.")