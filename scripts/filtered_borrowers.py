from pymongo import MongoClient, WriteConcern
import os,time,sys
from dotenv import load_dotenv
import pandas as pd
from pymongo import UpdateOne

load_dotenv()

DB_NAME = "lead_compass"

# MONGO_CONNECTION_URL = os.getenv("mongodb://localhost:27017")

client = MongoClient("mongodb://localhost:27017")

db = client[DB_NAME]

source_collection_name = sys.argv[1]
target_collection_name = sys.argv[2]
project_id = sys.argv[3]

# source_collection_name = "test_final_flattened_transactions"
# target_collection_name = "test_filtered_borrowers"
# project_id = "65a3ff7be0d06b46084ef135"

source_collection = db[source_collection_name]
target_collection = db[target_collection_name]


source_collection.create_index([("LC_Borrower", 1)])


def get_current_time():
    return time.time()

st = get_current_time()
 
pipeline = [
    {
        "$match": {"LC_Borrower":{"$ne":""},"ProjectId":project_id}
        # "$match": {"LC_Borrower":{"$ne":""}}
    },
    {      
        "$group": {
            "_id": "$LC_Borrower",
            "LC_TotalLoanAmount": {"$sum": "$LC_PartialLoanAmount"},
            "LC_NumberOfLoans": {"$sum": 1},
            "FIPSCodeSet": {"$addToSet": "$FIPSCode"},
            "PropertyStateSet" : {"$addToSet":"$PropertyState"},
            "DPIDSet": {"$addToSet": "$DPID"},
            "LC_LatestTransactionDate": {"$max": "$OriginalDateOfContract"},
            "LC_Transactions": {
                "$push": {
                    "FIPSCode": "$FIPSCode",
                    "Source": "$LC_Source",
                    "TransactionId": "$TransactionId",
                    "DPID": "$DPID",
                    "OriginalDateOfContract": "$OriginalDateOfContract"
                }
            },
            "LC_BorrowerFullAddressSet": {"$addToSet": "$LC_BorrowerFullAddress"},
            "LC_BorrowerFullAddressJsonSet":{
                "$addToSet": {
                    "BorrowerMailFullStreetAddress": "$BorrowerMailFullStreetAddress",
                    "BorrowerMailUnitNumber": "$BorrowerMailUnitNumber",
                    "BorrowerMailUnitType": "$BorrowerMailUnitType",
                    "BorrowerMailZip4": "$BorrowerMailZip4",
                    "BorrowerMailZipCode": "$BorrowerMailZipCode",
                    "BorrowerMailCity": "$BorrowerMailCity",
                    "BorrowerMailState": "$BorrowerMailState"
                }
            },
            "ProjectId": {"$first":"$ProjectId"},
        }
    },
    {
        "$addFields": {
            "LC_TotalNumberOfPropertyTransactions": {"$size": "$DPIDSet"},
            "LC_Borrower": "$_id"
        }
    },
    {
        "$project": {
            "_id": 0,
            "DPIDSet": 0
        }
    }
    # {
    #     "$out": target_collection_name
    # }
]
 
 
# source_collection.aggregate(pipeline)
 
# # source_collection.aggregate(pipeline, allowDiskUse=True)
 
# et = get_current_time()
 
# print(f"Time taken {et-st}")
 
# # Close the MongoDB connection
# client.close()


# Perform aggregation on the source collection
result = source_collection.aggregate(pipeline)

# Insert the newly fetched documents into the target collection
target_collection.insert_many(result)

et = get_current_time()

print(f"Time taken: {et - st}")

# Close the MongoDB connection
client.close()