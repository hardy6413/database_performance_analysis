import pandas as pd

from src.constants import FILEPATH
from src.gui import DatabaseOperationsApp
from src.repositories import mongo_repository, redis_repository, postgres_repository
from src.repositories.postgres_repository import postgres_conn


def initialize_databases():
    data = pd.read_csv(FILEPATH, low_memory=False)
    redis_repository.initialize_redis(data)
    mongo_repository.initialize_mongo(data)
    postgres_repository.initialize_postgres(data)


def clear_databases():
    redis_repository.execute_delete()
    mongo_repository.delete_all()
    postgres_repository.execute_delete()


if __name__ == '__main__':
    app = DatabaseOperationsApp()
    app.run()
    postgres_conn.close()
