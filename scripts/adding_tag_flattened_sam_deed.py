import time
from dotenv import load_dotenv, find_dotenv
import pandas as pd
from pymongo import UpdateOne
from pymongo import MongoClient
import sys

_ = load_dotenv(find_dotenv())
DB_NAME = "lead_compass"


COLLECTION_FLATTENED_TRANSACTION = sys.argv[1]
PROJECT_ID = sys.argv[2]



# COLLECTION_FLATTENED_TRANSACTION= "test_flattened_transactions"
# PROJECT_ID = 1

# MONGO_CONNECTION_URL = os.getenv("mongodb://localhost:27017")
def get_current_time():
    return time.time()

client = MongoClient("mongodb://localhost:27017")

# Accessing the database
db = client[DB_NAME]

# Access the raw collection DEED
collection_flattened_transaction = db[COLLECTION_FLATTENED_TRANSACTION]
# Access the raw collection SAM
# collection_final_flattened_transaction = db[COLLECTION_FINAL_FLATTENED_TRANSACTION]

project_id = PROJECT_ID

collection_flattened_transaction.create_index([("LC_Borrower", 1)])

print("Started tagging company_borrowers")

st = get_current_time()

trust_with_less_than_this_loan_will_be_rejected = 1000000
trust_with_less_than_this_many_property_transactions_will_be_rejected = 3

pipeline_c = [
    {
        "$match": {
            "ProjectId": project_id,
            "LC_Borrower": {"$regex": r"\btrust\b", "$options": "i"}
        }
    },
    {
        "$group": {
            "_id": "$LC_Borrower",
            "TotalPartialLoanAmount": {"$sum": "$LC_PartialLoanAmount"},
            "ListOfProperties": {"$addToSet": "$DPID"}
        }
    },
    {
        "$project": {
            "_id": 1,
            "LC_Borrower": "$_id",
            "ListOfProperties": 1,
            "ListOfPropertiesCount": {"$size": "$ListOfProperties"},
            "TotalPartialLoanAmount":1
        }
    },
    {
        "$match": {
            "$or": [
                {"TotalPartialLoanAmount": {"$lt": trust_with_less_than_this_loan_will_be_rejected}},
                {"ListOfPropertiesCount": {"$lt": trust_with_less_than_this_many_property_transactions_will_be_rejected}}
            ]
        }
    },
    {
        "$project": {
            "_id": 0,
            "LC_Borrower": "$LC_Borrower",
            "TotalPartialLoanAmount": 1,
            "ListOfProperties": 1,
            "ListOfPropertiesCount": 1
        }
    }
]


result = list(collection_flattened_transaction.aggregate(pipeline_c))

borrower_list_with_name_trust_net_loan_amount_less_than_million = [entry["LC_Borrower"] for entry in result]

# Update each row in the collection
for borrower in borrower_list_with_name_trust_net_loan_amount_less_than_million:
    collection_flattened_transaction.update_many(
        {"LC_Borrower": borrower},
        {"$set": {"LC_IsTrustAndNetLoanLessThanMillionOrLessThanThreePropertyTransactions": "Y"}}
    )

# Update rows where Borrower is not in the list
collection_flattened_transaction.update_many(
    {"LC_Borrower": {"$nin": borrower_list_with_name_trust_net_loan_amount_less_than_million}},
    {"$set": {"LC_IsTrustAndNetLoanLessThanMillionOrLessThanThreePropertyTransactions": "N"}}
)

et = get_current_time()

print(f"Time taken in tagging company borrowers is {et-st} seconds")