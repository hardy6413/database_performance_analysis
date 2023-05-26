import psycopg2
import pymongo
import pandas as pd
import time
from sqlalchemy import create_engine

postgresConn = psycopg2.connect(host="localhost", dbname="pz", user="pz", password="pz")
postgresConn.autocommit = True
alchemyDb = create_engine("postgresql://pz:pz@localhost:5432/pz")
alchemyPostgresConn = alchemyDb.connect()

mongoClient = pymongo.MongoClient("mongodb://%s:%s@localhost:27017/" % ("admin", "admin"))
mongodb = mongoClient["test"]
mongoCollection = mongodb["data"]

# tak sie dodaje do postgresa cos
#postgresCursor = postgresConn.cursor()
#postgresCursor.execute("""CREATE TABLE IF NOT EXISTS person (
#id INT PRIMARY KEY )""")
#postgresConn.commit()
#postgresCursor.close()
#postgresConn.close()

# mierzenie czasau
# start = time.time()
# print("hello")
# end = time.time()
# print(end - start)


if __name__ == '__main__':
    # wczytanie danych dla paru gb bedzie musialo byc z jakimis chunkami
    chunk = pd.read_csv('data/data_test_3.csv', low_memory=False)
    # dataset = pd.concat(chunk)
    # print(dataset.sample(2))
    chunk.to_sql('Data', if_exists='replace', index=False, con=alchemyPostgresConn)
    mongoCollection.insert_many(chunk.to_dict("records"))
    print(chunk.head(5))
    print("im working")
