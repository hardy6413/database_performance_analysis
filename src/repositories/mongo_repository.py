import ast
import os

import pandas as pd
from matplotlib import pyplot as plt

from src.constants import SCHEMA_NAME
from src.database_connection import get_mongo_client
import time

mongo_client = get_mongo_client()
collection = mongo_client[SCHEMA_NAME]

selectDurations = []
deleteDurations = []
updateDurations = []
countDurations = []
meanDurations = []
wordDurations = []

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

    start = time.time()
    x = collection.find(dict(stmt))
    end = time.time()
    selectDurations.append(end - start)
    df = pd.DataFrame(list(x))
    return df


def execute_delete(stmt):
    if not isinstance(stmt, dict):
        stmt = ast.literal_eval(stmt)

    start = time.time()
    x = collection.delete_many(dict(stmt))
    end = time.time()
    deleteDurations.append(end - start)


def execute_update(stmt):
    stmt, new_values = stmt.split(';', 1)
    if not isinstance(stmt, dict):
        stmt = ast.literal_eval(stmt)

    if not isinstance(new_values, dict):
        new_values = ast.literal_eval(new_values)

    start = time.time()
    x = collection.update_many(dict(stmt), dict(new_values))
    end = time.time()
    updateDurations.append(end - start)


def execute_count(stmt):
    stmt, new_values = stmt.split(';', 1)
    if not isinstance(stmt, dict):
        stmt = ast.literal_eval(stmt)

    start = time.time()
    x = collection.find(dict(stmt), {new_values: 1})
    end = time.time()
    countDurations.append(end - start)
    df = pd.DataFrame(list(x))
    count = len(df.index)

    plt.figure()
    plt.xlabel(df.columns[1])
    plt.ylabel("count")
    plt.hist(df[df.columns[1]])
    os.makedirs(os.path.dirname('results/'), exist_ok=True)
    plt.savefig('./results/hist.png')
    return count


def execute_mean(stmt):
    stmt, new_values = stmt.split(';', 1)
    if not isinstance(stmt, dict):
        stmt = ast.literal_eval(stmt)
    start = time.time()
    x = collection.find(dict(stmt), {new_values: 1})
    end = time.time()
    countDurations.append(end - start)
    df = pd.DataFrame(list(x))

    means = {}
    mediane = {}
    for col in df.columns:
        means.update({col: df[col].mean()})
        mediane.update({col: df[col].median()})

    end = time.time()
    meanDurations.append(end - start)
    return means, mediane


