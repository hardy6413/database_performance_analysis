import psycopg2
import pymongo
from pymongo import MongoClient

postgresConn = psycopg2.connect(host="localhost", dbname="pz", user="pz", password="pz")
postgresCursor = postgresConn.cursor()

mongoClient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = mongoClient["logs"]

if __name__ == '__main__':
    print("im working")
