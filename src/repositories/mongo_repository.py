import ast

import pandas as pd

from src.constants import SCHEMA_NAME
from src.database_connection import get_mongo_client

mongo_client = get_mongo_client()
collection = mongo_client[SCHEMA_NAME]


def initialize_mongo(data):
    records = data.to_dict(orient='records')
    collection.insert_many(records)


def delete_all():
    collection.delete_many(filter={})


def close_connection():
    mongo_client.close()


def execute_query(stmt):
    if not isinstance(stmt, dict):
        stmt = ast.literal_eval(stmt)
    x = collection.find(dict(stmt))
    df = pd.DataFrame(list(x))
    return df


def execute_delete(stmt):
    if not isinstance(stmt, dict):
        stmt = ast.literal_eval(stmt)
    x = collection.delete_many(dict(stmt))


def execute_update(stmt):
    stmt, new_values = stmt.split(',', 1)
    if not isinstance(stmt, dict):
        stmt = ast.literal_eval(stmt)

    if not isinstance(new_values, dict):
        new_values = ast.literal_eval(new_values)

    x = collection.update_many(dict(stmt), dict(new_values))
