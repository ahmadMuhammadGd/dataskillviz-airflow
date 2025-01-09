from modules.vectorizer.fasttext import CustomFasttext
# from modules.vectorizer.word2vec import Word2VecVectorizer
from modules.interfaces.interfaces import Json_interface, Sql_interface
from global_variables import (
    skills_cleaned_json_path,
    model_directory,
    connection_string,
    dimensions_sql_path,
    fasttext_model
)
import psycopg2
from psycopg2.extras import execute_values
import numpy as np


def get_tech_skill_embeddings(vectorizer, json_path: str) -> list[tuple[str, np.ndarray]]:
    """
    Extract domain-specific vectors based on a JSON seed file.
    Args:
        vectorizer: An instance of Vectorizer.
        json_path: Path to the JSON file containing seed words.
    Returns:
        A list of tuples, where each tuple contains a word and its embedding.
    """
    user_interface = Json_interface()
    skills = user_interface.read(json_path)
    print(type(skills), skills)
    return [(skill, vectorizer.get_vector(skill)) for skill in skills]

def pycopg_execute_sql(query:str, params:any=None):
    with psycopg2.connect(connection_string) as conn:
        with conn.cursor() as cur:
            if params:
                execute_values(cur, query, params)
            else:
                cur.execute(query)
            conn.commit()
    
if __name__ == "__main__":
    vectorizer = CustomFasttext(fasttext_model)
    embeddings = get_tech_skill_embeddings(vectorizer, skills_cleaned_json_path)
    sql_interace = Sql_interface()
    sql_queries = sql_interace.reads(dimensions_sql_path)
    for query in sql_queries:
        print(f"""
              
----------------------------------

🤸 running:
{query}
        
        """)
        if "insert into warehouse.tags_dim" in query.lower():
            vectors = [(word, list(map(float, vector))) for word, vector in embeddings]
            pycopg_execute_sql(query, vectors)
        else:
            pycopg_execute_sql(query)
        print("status: ✅ success!")