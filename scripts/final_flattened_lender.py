from pymongo import MongoClient
import os
import time
from dotenv import load_dotenv
import sys

load_dotenv()

# MONGO_CONNECTION_URL = os.getenv("mongodb://localhost:27017")
# client = MongoClient("mongodb://localhost:27017")


def get_current_time():
    return time.time()

def if_null_with_conditional(field, fallback_field, fallback_value=None):
    return {
        "$ifNull": [
            f"${field}",
            {
                "$cond": {
                    "if": {f"$gt": [f"${fallback_field}", None]},
                    "then": f"${fallback_field}",
                    "else": fallback_value
                }
            }
        ]
    }

def get_fields_to_select():
    fields_to_select = {
        "LoanAmount": if_null_with_conditional("LoanAmount", "ConcurrentTDLoanAmount"),

        "TransactionId": "$PID",
        
        "PropertyFullStreetAddress": "$PropertyFullStreetAddress",
        "PropertyZipCode": "$PropertyZipCode",
        "PropertyZip4": "$PropertyZip4",
        "PropertyUnitType": "$PropertyUnitType",
        "PropertyUnitNumber": "$PropertyUnitNumber",
        "PropertyState": "$PropertyState",
        "PropertyCityName": "$PropertyCityName",

        "LC_PropertyFullAddress": "$LC_PropertyFullAddress",

        "LenderMailFullStreetAddress": "$LenderMailFullStreetAddress",
        "LenderMailZipCode": "$LenderMailZipCode",
        "LenderMailZip4":  "$LenderMailZip4",
        "LenderMailUnitType":"$LenderMailUnitType",
        "LenderMailUnitNumber":  "$LenderMailUnit",
        "LenderMailState": "$LenderMailState",
        "LenderMailCity": "$LenderMailCity",

        "LC_LenderFullAddress": "$LC_LenderFullAddress",

        "OriginalDateOfContract": "$OriginalDateOfContract",
        "LenderNameBeneficiary": if_null_with_conditional("LenderNameBeneficiary", "ConcurrentTDLenderName"),
        "TransactionId": "$PID",

        "FIPSCode": '$FIPSCode',
        "APN": '$APN',
        "DPID": '$DPID',
        "_id": 0,
        "LC_Source":"$LC_Source",
        "ProjectId": "$ProjectId",
    }
    return fields_to_select

def get_pipeline(filter_criteria, fields_to_select, target_collection_name):
    pipeline = [
        {"$match": filter_criteria},
        {"$project": fields_to_select},
        {"$merge": {"into": target_collection_name, "whenMatched": "merge"}}
    ]
    return pipeline

def aggregate_and_insert(db, src_collection_name, pipeline):
    src_collection = db[src_collection_name]
    src_collection.aggregate(pipeline)

def main():
    DB_NAME = "lead_compass"

    # FLATTENED_TRANSACTIONS = sys.argv[1]
    # FINAL_FLATTENED_TRANSACTIONS = sys.argv[2]
    # PROJECT_ID = sys.argv[3]
    
    FLATTENED_TRANSACTIONS = "test_flattened_lender_transactions"
    FINAL_FLATTENED_TRANSACTIONS = "test_final_flattened_lender_transactions"
    PROJECT_ID = "65aa4aeedae0666f0977bc57"

    client = MongoClient("mongodb://localhost:27017")
    db = client[DB_NAME]

    project_id = PROJECT_ID

    st = get_current_time()
            
    filter_criteria = {"ProjectId": project_id}
    # filter_criteria = {"LC_IsTrustAndNetLoanLessThanMillionOrLessThanThreePropertyTransactions": "N"}


    fields_to_select = get_fields_to_select()

    pipeline= get_pipeline(filter_criteria, fields_to_select, FINAL_FLATTENED_TRANSACTIONS)

    aggregate_and_insert(db, FLATTENED_TRANSACTIONS, pipeline)

    et = get_current_time()

    print(f"Total time taken in dumping filtered data into single table is {et-st} seconds")
    client.close()

   
main()
