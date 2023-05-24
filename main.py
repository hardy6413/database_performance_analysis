import psycopg2
import pymongo
from pymongo import MongoClient

postgresConn = psycopg2.connect(host="localhost", dbname="pz", user="pz", password="pz")

mongoClient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = mongoClient["logs"]

#tak sie dodaje do postgresa cos
#postgresCursor = postgresConn.cursor()
#postgresConn.commit()
#postgresCursor.close()
#postgresConn.close()

if __name__ == '__main__':
    print("im working")
