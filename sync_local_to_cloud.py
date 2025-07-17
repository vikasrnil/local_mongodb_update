import time
import pymongo
from pymongo import MongoClient
from pymongo import errors

local_client = pymongo.MongoClient("mongodb://localhost:27017/")
local_collection = local_client["mydatabase1"]["realtime_data"]

atlas_client = pymongo.MongoClient(
    "mongodb+srv://tanyaarora:tanyaarora@cluster0.cffvebq.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
)
atlas_collection = atlas_client["mydatabase1"]["realtime_data"]
#atlas_collection.create_index("timestamp", unique=True)

last_synced_doc = atlas_collection.find_one(sort=[("datetime", -1)])

if last_synced_doc:
    last_timestamp = last_synced_doc["datetime"]
else:
    last_timestamp = None


while True:
    start=time.time()
    try:
        if last_timestamp:
            query = {"datetime": {"$gt": last_timestamp}}
        else:
            query = {}
        #new_entry = local_collection.find(query).sort("timestamp", 1)
        new_entry = local_collection.find(query)
        start=time.time()
        for doc in new_entry:
            value_insert = doc.copy()
            value_insert.pop("_id", None)

            try:
                atlas_collection.insert_one(value_insert)
                last_timestamp = value_insert["datetime"]

            except errors.DuplicateKeyError:
                continue
            except Exception:
                continue
    except Exception:
        pass
