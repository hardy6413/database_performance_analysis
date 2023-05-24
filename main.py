import psycopg2
import pymongo

postgresConn = psycopg2.connect(host="localhost", dbname="pz", user="pz", password="pz")

mongoClient = pymongo.MongoClient("mongodb://%s:%s@localhost:27017/" % ("admin", "admin"))
mongodb = mongoClient["test"]
mongoCollection = mongodb["test"]
post = {"id": 1, "name": "test"}
mongoCollection.insert_one(post)

# tak sie dodaje do postgresa cos
# postgresCursor = postgresConn.cursor()
# postgresCursor.execute("""CREATE TABLE IF NOT EXISTS person (
# id INT PRIMARY KEY )""")
# postgresConn.commit()
# postgresCursor.close()
# postgresConn.close()

if __name__ == '__main__':
    print("im working")
