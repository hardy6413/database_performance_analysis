import ast
import os

import pandas as pd
from matplotlib import pyplot as plt

from src.constants import SCHEMA_NAME
from src.database_connection import get_mongo_client
import time

from src.statistics_helpers import calculate_mean_and_median

mongo_client = get_mongo_client()
collection = mongo_client[SCHEMA_NAME]

select_durations = []
delete_durations = []
update_durations = []
insert_durations = []
count_durations = []
mean_durations = []
word_durations = []


def initialize_mongo(data):
    records = data.to_dict(orient='records')
    collection.insert_many(records)


def delete_all():
    collection.delete_many(filter={})


def close_connection():
    mongo_client.close()


def execute_insert(stmt):
    if not isinstance(stmt, dict):
        stmt = ast.literal_eval(stmt)
    start = time.time()
    collection.insert_one(dict(stmt))
    end = time.time()
    insert_durations.append(end - start)


def execute_query(stmt):
    if not isinstance(stmt, dict):
        stmt = ast.literal_eval(stmt)

    start = time.time()
    x = collection.find(dict(stmt))
    end = time.time()
    select_durations.append(end - start)
    df = pd.DataFrame(list(x))
    return df


def execute_delete(stmt):
    if not isinstance(stmt, dict):
        stmt = ast.literal_eval(stmt)

    start = time.time()
    collection.delete_one(dict(stmt))
    end = time.time()
    delete_durations.append(end - start)


def execute_update(stmt):
    stmt, new_values = stmt.split(';', 1)
    if not isinstance(stmt, dict):
        stmt = ast.literal_eval(stmt)

    if not isinstance(new_values, dict):
        new_values = ast.literal_eval(new_values)

    start = time.time()
    collection.update_many(dict(stmt),  {"$set": dict(new_values)})
    end = time.time()
    update_durations.append(end - start)


def execute_count(stmt):
    stmt, new_values = stmt.split(';', 1)
    if not isinstance(stmt, dict):
        stmt = ast.literal_eval(stmt)

    start = time.time()
    x = collection.find(dict(stmt), {new_values: 1})
    end = time.time()
    count_durations.append(end - start)
    df = pd.DataFrame(list(x))
    count = len(df.index)

    plt.figure()
    plt.xlabel(df.columns[1])
    plt.ylabel("count")
    plt.hist(df[df.columns[1]])
    os.makedirs(os.path.dirname('results/'), exist_ok=True)
    plt.savefig('./results/hist_redis.png')
    return count


def execute_mean(stmt):
    cols = stmt.split(';')
    stmt = cols[0]
    if not isinstance(stmt, dict):
        stmt = ast.literal_eval(stmt)

    t = {}
    for col in cols:
        t.update({col: 1})
    start = time.time()
    x = collection.find(dict(stmt), t)
    end = time.time()
    count_durations.append(end - start)
    res = pd.DataFrame(list(x))
    means, median = calculate_mean_and_median(res)
    end = time.time()
    mean_durations.append(end - start)
    return means, median


def execute_word(stmt):
    stmt, col, word = stmt.split(';', 2)
    if not isinstance(stmt, dict):
        stmt = ast.literal_eval(stmt)

    start = time.time()
    x = collection.find(dict(stmt), {col: 1})
    df = pd.DataFrame(list(x))
    amount = df[df.columns[1]].str.count(str(word)).sum()
    end = time.time()
    word_durations.append(end - start)
    return amount
