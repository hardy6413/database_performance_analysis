import pandas as pd

from src.constants import FILEPATH
from src.repositories.mongo_repository import initialize_mongo
from src.repositories.posgres_repository import initialize_postgres
from src.repositories.redis_repository import initialize_redis


# mierzenie czasu
# start = time.time()
# print("hello")
# end = time.time()
# print(end - start)

def initialize_databases():
    data = pd.read_csv(FILEPATH, low_memory=False)
    initialize_redis(data)
    initialize_mongo(data)
    initialize_postgres(data)


if __name__ == '__main__':
    initialize_databases()

    # GUI
    # app = DatabaseOperationsApp()
    # app.run()

    # dataset = pd.concat(chunk)
    # print(dataset.sample(2))
    # add_to_postgres(chunk)
    # add_to_mongo(chunk)
    # delete_all_from_postgres()
    # delete_all_from_mongo()
    # print(chunk.head(5))
    # print("im working")
    # redis_conn.set('key', 'value')
    # value = redis_conn.get('key')
    # print(value)
