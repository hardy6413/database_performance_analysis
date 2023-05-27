import psycopg2
import pymongo
import redis
from sqlalchemy import create_engine


def get_postgres_connection():
    # Ustawienia połączenia PostgreSQL
    host = "localhost"
    port = 5431
    database = "pz"
    user = "pz"
    password = "pz"

    # Nawiązanie połączenia
    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=database,
        user=user,
        password=password
    )
    conn.autocommit = True
    return conn


def get_mongo_connection():
    # Ustawienia połączenia MongoDB
    mongo_client = pymongo.MongoClient("mongodb://%s:%s@localhost:27017/" % ("admin", "admin"))
    mongodb = mongo_client["test"]
    return mongodb


def get_redis_connection():
    # Ustawienia połączenia Redis
    redis_connection = redis.Redis(host='localhost', port=6379)
    return redis_connection


def get_alchemy_engine():
    # Ustawienia połączenia SQLAlchemy
    host = "localhost"
    port = 5431
    database = "pz"
    user = "pz"
    password = "pz"

    # Tworzenie silnika SQLAlchemy
    alchemy_db = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")
    return alchemy_db.connect()
