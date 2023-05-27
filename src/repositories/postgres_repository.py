import pandas as pd

from src.constants import SCHEMA_NAME
from src.database_connection import get_alchemy_engine, get_postgres_connection

postgres_conn = get_postgres_connection()
alchemy_conn = get_alchemy_engine()


def initialize_postgres(data):
    data.to_sql(SCHEMA_NAME, if_exists='append', index=False, con=alchemy_conn)


def delete_all():
    cursor = postgres_conn.cursor()
    query = "DELETE FROM " + SCHEMA_NAME
    cursor.execute(query)
    postgres_conn.commit()
    cursor.close()

def execute_query(stmt):
    res = pd.read_sql_query(stmt, postgres_conn)
    return res


def get_by_id(element_id):
    pass
