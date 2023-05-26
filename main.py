import pandas as pd
import psycopg2
import pymongo
from sqlalchemy import create_engine, delete, MetaData

postgresConn = psycopg2.connect(host="localhost", dbname="pz", user="pz", password="pz")
postgresConn.autocommit = True
alchemyDb = create_engine("postgresql://pz:pz@localhost:5432/pz")
alchemyPostgresConn = alchemyDb.connect()

mongoClient = pymongo.MongoClient("mongodb://%s:%s@localhost:27017/" % ("admin", "admin"))
mongodb = mongoClient["test"]
mongoCollection = mongodb["data"]


meta = MetaData(bind=alchemyDb)
MetaData.reflect(meta)

# tak sie dodaje do postgresa cos
#postgresCursor = postgresConn.cursor()
# postgresCursor.execute("""CREATE TABLE IF NOT EXISTS person (
# id INT PRIMARY KEY )""")
# postgresConn.commit()
# postgresCursor.close()
# postgresConn.close()

# mierzenie czasau
# start = time.time()
# print("hello")
# end = time.time()
# print(end - start)

def addToPostgres(data):
    data.to_sql('data', if_exists='append', index=False, con=alchemyPostgresConn)


def deleteAllFromPostgres():
    postgresCursor = postgresConn.cursor()
    postgresCursor.execute("""DELETE from "Data" where TRUE""")
    postgresConn.commit()
    postgresCursor.close()
    postgresConn.close()


def deleteAllFromMongo():
    mongoCollection.delete_many(filter={})


def addToMongo(data):
    mongoCollection.insert_many(data.to_dict("records"))


if __name__ == '__main__':
    # wczytanie danych dla paru gb bedzie musialo byc z jakimis chunkami
    chunk = pd.read_csv('data/data_test_3.csv', low_memory=False)
    # dataset = pd.concat(chunk)
    # print(dataset.sample(2))
    deleteAllFromPostgres()
    deleteAllFromMongo()
    addToPostgres(chunk)
    addToMongo(chunk)
    print(chunk.head(5))
    print("im working")
