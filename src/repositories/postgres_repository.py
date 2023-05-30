import pandas as pd

from src.constants import SCHEMA_NAME
from src.database_connection import get_alchemy_engine, get_postgres_connection
import time

postgres_conn = get_postgres_connection()
alchemy_conn = get_alchemy_engine()


selectDurations = []
deleteDurations = []
updateDurations = []

def initialize_postgres(data):
    data.to_sql(SCHEMA_NAME, if_exists='append', index=False, con=alchemy_conn)


def delete_all():
    cursor = postgres_conn.cursor()
    query = "DELETE FROM " + SCHEMA_NAME
    cursor.execute(query)
    postgres_conn.commit()
    cursor.close()


def execute_query(stmt):
    start = time.time()
    res = pd.read_sql_query(stmt, postgres_conn)
    end = time.time()
    selectDurations.append(end - start)
    return res


def execute_delete(stmt):
    start = time.time()

    cursor = postgres_conn.cursor()
    cursor.execute(stmt)
    postgres_conn.commit()
    cursor.close()

    end = time.time()
    deleteDurations.append(end - start)


def close_connection():
    postgres_conn.close()


def execute_update(stmt):
    start = time.time()

    cursor = postgres_conn.cursor()
    cursor.execute(stmt)
    postgres_conn.commit()
    cursor.close()

    end = time.time()
    updateDurations.append(end - start)
