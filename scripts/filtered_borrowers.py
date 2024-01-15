from pymongo import MongoClient, WriteConcern
import os,time,sys
from dotenv import load_dotenv
import pandas as pd
from pymongo import UpdateOne

load_dotenv()

DB_NAME = "lead_compass"

MONGO_CONNECTION_URL = os.getenv("mongodb://localhost:27017")

client = MongoClient(MONGO_CONNECTION_URL)

db = client[DB_NAME]

# source_collection_name = sys.argv[1]
# target_collection_name = sys.argv[2]
# project_id = sys.argv[3]

source_collection_name = "test_final_flattened_transactions"
target_collection_name = "test_filtered_borrowers"
project_id = "65a3ff7be0d06b46084ef135"

source_collection = db[source_collection_name]
target_collection = db[target_collection_name]


source_collection.create_index([("LC_Borrower", 1)])


def get_current_time():
    return time.time()

st = get_current_time()



pipeline = [
    {
        "$match": {
            "ProjectId": project_id,
        }
    },
    {
        "$group": {
            "_id": "$LC_Borrower",
            "LC_TotalLoanAmount": {"$sum": "$LC_PartialLoanAmount"},
            "LC_NumberOfLoans": {"$sum": 1},
            "FIPSCodeSet": {"$addToSet": "$FIPSCode"},
            "DPIDSet": {"$addToSet": "$DPID"},
            "LC_BorrowerFullAddressSet": {"$addToSet": "$LC_BorrowerFullAddress"},
            "LC_LatestTransactionDate": {"$max": "$OriginalDateOfContract"},
            "LC_Transactions": {
                "$push": {
                    "FIPSCode": "$FIPSCode",
                    "Source": "$LC_Source",
                    "TransactionId": "$TransactionId",
                    "DPID": "$DPID"
                }
            },
            "ProjectId": {"$first":"$ProjectId"}
        }
    },
    {
        "$addFields": {
            "LC_TotalNumberOfPropertyTransactions": {"$size": "$DPIDSet"},
        }
    },
    {
        "$unwind": "$LC_Transactions"
    },
    {
        "$group": {
            "_id": {
                "LC_Borrower": "$_id",
                "DPID": "$LC_Transactions.DPID"
            },
            "LC_Transactions": {
                "$push": {
                    "FIPSCode": "$LC_Transactions.FIPSCode",
                    "Source": "$LC_Transactions.Source",
                    "TransactionId": "$LC_Transactions.TransactionId"
                }
            },
            "LC_TotalLoanAmount": {"$first": "$LC_TotalLoanAmount"},
            "LC_NumberOfLoans": {"$first": "$LC_NumberOfLoans"},
            "FIPSCodeSet": {"$first": "$FIPSCodeSet"},
            "LC_BorrowerFullAddressSet": {"$first": "$LC_BorrowerFullAddressSet"},
            "LC_LatestTransactionDate": {"$first": "$LC_LatestTransactionDate"},
            "LC_TotalNumberOfPropertyTransactions": {"$first": "$LC_TotalNumberOfPropertyTransactions"},
            "ProjectId": {"$first":"$ProjectId"}
        }
    },
    {
        "$group": {
            "_id": "$_id.LC_Borrower",
            "LC_Borrower": {"$first": "$_id.LC_Borrower"},
            "LC_TotalLoanAmount": {"$first": "$LC_TotalLoanAmount"},
            "LC_NumberOfLoans": {"$first": "$LC_NumberOfLoans"},
            "FIPSCodeSet": {"$first": "$FIPSCodeSet"},
            "LC_BorrowerFullAddressSet": {"$first": "$LC_BorrowerFullAddressSet"},
            "LC_LatestTransactionDate": {"$first": "$LC_LatestTransactionDate"},
            "LC_TotalNumberOfPropertyTransactions": {"$first": "$LC_TotalNumberOfPropertyTransactions"},
            "LC_Transactions": {
                "$push": {
                    "k": {"$toString": "$_id.DPID"},
                    "v": "$LC_Transactions"
                }
            },
            "ProjectId": {"$first":"$ProjectId"}
        }
    },
    {
        "$addFields": {
            "LC_Transactions": {"$arrayToObject": "$LC_Transactions"}
        }
    },
    {
        "$project": {
            "DPIDSet": 0
        }
    },
    {
        "$addFields": {
            "_id": "$$REMOVE"
        }
    },
    {
        "$out": target_collection_name
    }
]

source_collection.aggregate(pipeline)

et = get_current_time()

print(f"Time taken {et-st}")

client.close()