from modules.vectorizer.fasttext import CustomFasttext
from modules.interfaces.interfaces import Json_interface
from global_variables import skills_cleaned_json_path, fasttext_model
from itertools import chain
seed = [
    "sql"
    ,"python"
    ,"excel"
    ,"aws"
    ,"azure"
    ,"gcp"
    ,"apache"
    ,"spark"
    ,"kafka"
    ,"ssis"
    ,"airflow"
    ,"oracle"
    ,"microsoft"
    ,"psql"
    ,"looker"
    ,"google_sheet"
    ,"snowflake"
]

model = CustomFasttext(fasttext_model)

keywords = [model.most_similar(s, 10) for s in seed]
filtered = set(filter(lambda x: x[1] > 0.7, chain(*keywords)))
print(filtered)