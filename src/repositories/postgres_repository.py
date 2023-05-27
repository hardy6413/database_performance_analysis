from src.constants import SCHEMA_NAME
from src.database_connection import get_alchemy_engine, get_postgres_connection

postgres_conn = get_postgres_connection()
alchemy_conn = get_alchemy_engine()


def initialize_postgres(data):
    data.to_sql(SCHEMA_NAME, if_exists='append', index=False, con=alchemy_conn)


def delete_all():
    postgres_conn = get_postgres_connection()
    cursor = postgres_conn.cursor()
    query = "DELETE FROM " + SCHEMA_NAME
    cursor.execute(query)
    postgres_conn.commit()
    cursor.close()


def execute_query(query):
    cursor = postgres_conn.cursor()
    cursor.execute(query)


def close_connection():
    postgres_conn.close()
