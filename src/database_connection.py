import psycopg2
import pymongo
import redis
from sqlalchemy import create_engine

from src.constants import HOST, DATABASE_NAME, USER, PASSWORD, REDIS_PORT, POSTGRES_PORT, MONGO_PORT


def get_postgres_connection():
    conn = psycopg2.connect(host=HOST, port=POSTGRES_PORT, dbname=DATABASE_NAME, user=USER, password=PASSWORD)
    conn.autocommit = True
    return conn


def get_mongo_client():
    mongo_client = pymongo.MongoClient("mongodb://%s:%s@%s:%s/" % (USER, PASSWORD, HOST, MONGO_PORT))
    return mongo_client[DATABASE_NAME]


def get_redis_connection():
    redis_connection = redis.Redis(host=HOST, port=REDIS_PORT, db=0, decode_responses=True)
    return redis_connection


def get_alchemy_engine():
    alchemy_db = create_engine(f"postgresql://{USER}:{PASSWORD}@{HOST}:{POSTGRES_PORT}/{DATABASE_NAME}")
    return alchemy_db.connect()
