import json
import time
import pymongo
import os

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["mydatabase1"]
collection = db["realtime_data"]

while True:
        with open("data.json", "r") as f:
            try:
                entry = json.load(f)
                collection.insert_one(entry)
            except Exception as e:
                #print(f"JSON decode error: {e}")
                pass
            
