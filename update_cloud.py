import json
import time
import pymongo

client = pymongo.MongoClient(
    "mongodb+srv://tanyaarora:tanyaarora@cluster0.cffvebq.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
)
db = client["systemdata"]
collection = db["realtime_data"]

last_entry = None 

while True:
    try:
        with open("sysdata.json", "r") as f:
            current_entry = json.load(f)

        # Only insert if data is different from last inserted
        if current_entry != last_entry:
            collection.insert_one(current_entry)
            last_entry = current_entry
        else:
            pass

    except Exception as e:
        print(f"Error reading or inserting data: {e}")
