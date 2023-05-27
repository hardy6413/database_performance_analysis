from src.database_connection import get_mongo_client

mongo_client = get_mongo_client()
collection = mongo_client["footballers"]


def initialize_mongo(data):
    records = data.to_dict(orient='records')
    collection.insert_many(records)

