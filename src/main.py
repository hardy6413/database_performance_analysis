import pandas as pd

from src.constants import FILEPATH
from src.gui import DatabaseOperationsApp
from src.repositories import mongo_repository, redis_repository, postgres_repository
from src.repositories.postgres_repository import initialize_postgres, delete_all
from src.repositories.redis_repository import initialize_redis
from src.repositories.postgres_repository import postgres_conn


# mierzenie czasu
# start = time.time()
# print("hello")
# end = time.time()
# print(end - start)

def initialize_databases():
    data = pd.read_csv(FILEPATH, low_memory=False)
    redis_repository.initialize_redis(data)
    mongo_repository.initialize_mongo(data)
    postgres_repository.initialize_postgres(data)


def clear_databases():
    redis_repository.delete_all()
    mongo_repository.delete_all()
    postgres_repository.delete_all()


if __name__ == '__main__':
    app = DatabaseOperationsApp()
    app.run()
    postgres_conn.close()


