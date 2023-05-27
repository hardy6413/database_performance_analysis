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

