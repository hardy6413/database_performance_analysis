from src.database_connection import get_mongo_connection

mongo_conn = get_mongo_connection()





def initialize_mongo(data):
    mongo_conn.insert_many(data.to_dict("records"))


