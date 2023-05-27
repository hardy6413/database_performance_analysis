import pandas as pd
from database_connection import get_postgres_connection, get_mongo_connection, get_redis_connection, get_alchemy_engine


postgres_conn = get_postgres_connection()
alchemy_conn = get_alchemy_engine()
mongo_conn = get_mongo_connection()
redis_conn = get_redis_connection()


# mierzenie czasu
# start = time.time()
# print("hello")
# end = time.time()
# print(end - start)

def add_to_postgres(data):
    data.to_sql('data', if_exists='append', index=False, con=alchemy_conn)


def delete_all_from_postgres():
    postgres_cursor = postgres_conn.cursor()
    postgres_cursor.execute("""DELETE from "Data" where TRUE""")
    postgres_conn.commit()
    postgres_cursor.close()
    postgres_conn.close()


def delete_all_from_mongo():
    mongo_conn.delete_many(filter={})


def add_to_mongo(data):
    mongo_conn.insert_many(data.to_dict("records"))


if __name__ == '__main__':
    # wczytanie danych dla paru gb bedzie musialo byc z jakimis chunkami
    chunk = pd.read_csv('data/data_test_3.csv', low_memory=False)

    # dataset = pd.concat(chunk)
    # print(dataset.sample(2))
    add_to_postgres(chunk)
    add_to_mongo(chunk)
    # delete_all_from_postgres()
    # delete_all_from_mongo()
    print(chunk.head(5))
    print("im working")
    redis_conn.set('key', 'value')
    value = redis_conn.get('key')
    print(value)
