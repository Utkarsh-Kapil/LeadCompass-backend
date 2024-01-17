from pymongo import MongoClient
from dotenv import load_dotenv
import pandas as pd
from pymongo import UpdateOne
from bson.objectid import ObjectId
import os,time,sys


load_dotenv()

DB_NAME = "lead_compass"

COLLECTION_DEED = sys.argv[1]
COLLECTION_MORTGAGE = sys.argv[2]
PROJECT_ID = sys.argv[3]
DEED_IDS = sys.argv[4]
SAM_IDS = sys.argv[5]


project_id = PROJECT_ID
deed_inserted_ids = DEED_IDS
sam_inserted_ids = SAM_IDS

# COLLECTION_DEED = "test_deed"
# COLLECTION_MORTGAGE = "test_sam"
# PROJECT_ID = "65a79a690d174f3078f369da"


# Function to get current time
def get_current_time():
    return time.time()

st = get_current_time()
# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017")

# Accessing the database
db = client[DB_NAME]

# Access the raw collection DEED
collection_deed = db[COLLECTION_DEED]
# Access the raw collection SAM
collection_mortgage = db[COLLECTION_MORTGAGE]

collection_mortgage.update_many({"_id": {"$in": sam_inserted_ids}},
                                    {"$set": {"ProjectId": str(project_id)}})
            
collection_deed.update_many({"_id": {"$in": deed_inserted_ids}},
                        {"$set": {"ProjectId": str(project_id)}})

et = get_current_time()

print(f"Total time taken in updating project ids of sam and deed is {et-st} seconds")