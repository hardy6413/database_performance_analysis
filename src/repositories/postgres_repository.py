import os
import time

import pandas as pd
from matplotlib import pyplot as plt

from src.constants import SCHEMA_NAME, SELECT_OPERATION, WHERE, DELETE_OPERATION, UPDATE_OPERATION, \
    TABLE_NAME, SET, FROM
from src.database_connection import get_alchemy_engine, get_postgres_connection

postgres_conn = get_postgres_connection()
alchemy_conn = get_alchemy_engine()

selectDurations = []
deleteDurations = []
updateDurations = []
countDurations = []
meanDurations = []
wordDurations = []


def initialize_postgres(data):
    data.to_sql(SCHEMA_NAME, if_exists='append', index=False, con=alchemy_conn)


def delete_all():
    cursor = postgres_conn.cursor()
    query = DELETE_OPERATION + FROM + SCHEMA_NAME
    cursor.execute(query)
    postgres_conn.commit()
    cursor.close()


def execute_query(stmt):
    query = SELECT_OPERATION + stmt + FROM + TABLE_NAME
    start = time.time()
    res = pd.read_sql_query(query, postgres_conn)
    end = time.time()
    selectDurations.append(end - start)
    return res


def execute_delete(stmt):
    query = DELETE_OPERATION + FROM + TABLE_NAME + WHERE + stmt
    start = time.time()

    cursor = postgres_conn.cursor()
    cursor.execute(query)
    postgres_conn.commit()
    cursor.close()

    end = time.time()
    deleteDurations.append(end - start)


def execute_update(stmt):
    query = UPDATE_OPERATION + TABLE_NAME + SET + stmt
    start = time.time()

    cursor = postgres_conn.cursor()
    cursor.execute(query)
    postgres_conn.commit()
    cursor.close()

    end = time.time()
    updateDurations.append(end - start)


def close_connection():
    postgres_conn.close()


def execute_count(stmt):
    start = time.time()
    res = pd.read_sql_query(stmt, postgres_conn)
    count = len(res.index)
    end = time.time()
    countDurations.append(end - start)
    plt.figure()
    plt.xlabel(res.columns[0])
    plt.ylabel("count")
    plt.hist(res[res.columns[0]])
    os.makedirs(os.path.dirname('results/'), exist_ok=True)
    plt.savefig('./results/hist.png')
    return count


def execute_mean(stmt):
    start = time.time()
    res = pd.read_sql_query(stmt, postgres_conn)
    means = {}
    median = {}
    for col in res.columns:
        means.update({col: res[col].mean()})
        median.update({col: res[col].median()})

    end = time.time()
    meanDurations.append(end - start)
    return means, median


def execute_word(stmt):
    stmt, new_values = stmt.split(';', 1)
    start = time.time()
    res = pd.read_sql_query(stmt, postgres_conn)
    amount = res[res.columns[0]].str.count(str(new_values)).sum()
    end = time.time()
    wordDurations.append(end - start)
    return amount
