from src.constants import SCHEMA_NAME
from src.database_connection import get_mongo_client
import pandas as pd

mongo_client = get_mongo_client()
collection = mongo_client[SCHEMA_NAME]


def initialize_mongo(data):
    records = data.to_dict(orient='records')
    collection.insert_many(records)


def delete_all():
    collection.delete_many(filter={})


def get_by_id(element_id):
    pass


def execute_query(stmt):
    x = collection.find(stmt)
    df = pd.DataFrame(list(x))
    return df