from pymongo import MongoClient

database = 'weather_db'
table = 'weather_data'

def get_mongo_client():
    client = MongoClient('localhost', 27017)
    return client[database][table]

def fetch_all_data():
    db = get_mongo_client()
    return list(db.find({}))
