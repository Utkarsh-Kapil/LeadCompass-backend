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

# source_collection_name = sys.argv[1]
# target_collection_name = sys.argv[2]
# project_id = sys.argv[3]

source_collection_name = "test_final_flattened_lender_transactions"
target_collection_name = "test_filtered_lenders"
project_id = "65aa4aeedae0666f0977bc57"

source_collection = db[source_collection_name]
target_collection = db[target_collection_name]


source_collection.create_index([("LenderNameBeneficiary", 1)])


def get_current_time():
    return time.time()

st = get_current_time()
 
pipeline = [
    {
        "$match": {"LenderNameBeneficiary":{"$ne":""},"ProjectId":project_id}
        # "$match": {"LC_Borrower":{"$ne":""}}
    },
    {      
        "$group": {
            "_id": "$LenderNameBeneficiary",
            "LC_TotalLoanAmount": {"$sum": "$LoanAmount"},
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
            "LC_LenderFullAddressSet": {"$addToSet": "$LC_LenderFullAddress"},
            "LC_LenderFullAddressJsonSet":{
                "$addToSet": {
                    "LenderMailFullStreetAddress": "$LenderMailFullStreetAddress",
                    "LenderMailUnitNumber": "$LenderMailUnitNumber",
                    "LenderMailUnitType": "$LenderMailUnitType",
                    "LenderMailZip4": "$LenderMailZip4",
                    "LenderMailZipCode": "$LenderMailZipCode",
                    "LenderMailCity": "$LenderMailCity",
                    "LenderMailState": "$LenderMailState"
                }
            },
            "ProjectId": {"$first":"$ProjectId"},
        }
    },
    {
        "$addFields": {
            "LC_TotalNumberOfPropertyTransactions": {"$size": "$DPIDSet"},
            "LenderNameBeneficiary": "$_id"
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