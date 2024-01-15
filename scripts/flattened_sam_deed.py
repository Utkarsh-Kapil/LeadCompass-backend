from pymongo import MongoClient
import os,sys
import time
from dotenv import load_dotenv
from pymongo import InsertOne

load_dotenv()

DB_NAME = "lead_compass"


FILTER_DEED = sys.argv[1]
FILTER_SAM = sys.argv[2]
FLATTENED_TRANSACTIONS = sys.argv[3]
PROJECT_ID = sys.argv[4]

# FILTER_DEED = "test_filtered_deed"
# FILTER_SAM = "test_filtered_sam"
# FLATTENED_TRANSACTIONS = "test_flattened_transactions"
# PROJECT_ID = 1



MONGO_CONNECTION_URL = os.getenv("mongodb://localhost:27017")

client = MongoClient("mongodb://localhost:27017")

# Access a specific database (replace 'your_database' with your actual database name)
db = client[DB_NAME]

collection_filter_deed = db[FILTER_DEED]
# Access the raw collection SAM
collection_filter_sam = db[FILTER_SAM]

collection_flattened_transaction = db[FLATTENED_TRANSACTIONS]
project_id = PROJECT_ID

def get_current_time():
    return time.time()


batch_size = 1000

def flattening_collection(collection,source):
    count = 0
    cursor = collection.find({"ProjectId":project_id})
    # cursor = collection.find({})
    print("Done filtering")

    to_insert_company = []

    for document in cursor:
        count += 1
        if "_id" in document:
            del document["_id"]

        if count%batch_size==0:
            print(count)
            collection_flattened_transaction.bulk_write(to_insert_company)

            to_insert_company = []

        loan_amount_field = "ConcurrentTDLoanAmount" if source == "DEED" else "LoanAmount"

        borrower_full_street_address_field = "BuyerMailFullStreetAddress"
        borrower_mail_zip_code_field = "BuyerMailZipCode" if source == "DEED" else "BorrowerMailZipCode"
        borrower_mail_zip4_field = "BuyerMailZip4" if source == "DEED" else "BorrowerMailZip4"
        borrower_mail_unit_type_field = "BuyerMailUnitType" if source == "DEED" else "BorrowerMailUnitType"
        borrower_mail_unit_number_field = "BuyerMailUnitNumber" if source == "DEED" else "BorrowerMailUnitNumber"
        borrower_mail_state_field = "BuyerMailState" if source == "DEED" else "BorrowerMailState"
        borrower_mail_city_field = "BuyerMailCity" if source == "DEED" else "BorrowerMailCity"

        borrower_full_street_address_value = str(document[borrower_full_street_address_field]) if document[borrower_full_street_address_field] else "NA"
        borrower_mail_zip_code_value = str(document[borrower_mail_zip_code_field]) if document[borrower_mail_zip_code_field] else "NA"
        borrower_mail_zip4_value = str(document[borrower_mail_zip4_field]) if document[borrower_mail_zip4_field] else "NA"
        borrower_mail_unit_type_value = str(document[borrower_mail_unit_type_field]) if document[borrower_mail_unit_type_field] else "NA"
        borrower_mail_unit_number_value = str(document[borrower_mail_unit_number_field]) if document[borrower_mail_unit_number_field] else "NA"
        borrower_mail_state_value = str(document[borrower_mail_state_field]) if document[borrower_mail_state_field] else "NA"
        borrower_mail_city_value = str(document[borrower_mail_city_field]) if document[borrower_mail_city_field] else "NA"


        document["LC_BorrowerFullAddress"] = "BorrowerMailFullStreetAddress: " + borrower_full_street_address_value +  ", BorrowerMailZipCode: " + borrower_mail_zip_code_value + ", BorrowerMailZip4: " + borrower_mail_zip4_value + ", BorrowerMailUnitType: " + borrower_mail_unit_type_value + ", BorrowerMailUnitNumber: " + borrower_mail_unit_number_value + ", BorrowerMailState: " + borrower_mail_state_value + ", BorrowerMailCity: " + borrower_mail_city_value if document[borrower_full_street_address_field] else "NA"


        property_full_street_address_value = str(document["PropertyFullStreetAddress"]) if document["PropertyFullStreetAddress"] else "NA"
        property_zip_code_value = str(document["PropertyZipCode"]) if document["PropertyZipCode"] else "NA"
        property_zip4_value = str(document["PropertyZip4"]) if document["PropertyZip4"] else "NA"
        property_unit_type_value = str(document["PropertyUnitType"]) if document["PropertyUnitType"] else "NA"
        property_unit_number_value = str(document["PropertyUnitNumber"]) if document["PropertyUnitNumber"] else "NA"
        property_unit_state_value = str(document["PropertyState"]) if document["PropertyState"] else "NA"
        property_city_name_value = str(document["PropertyCityName"]) if document["PropertyCityName"] else "NA"

        document["LC_PropertyFullAddress"] =  "PropertyFullStreetAddress: " + property_full_street_address_value + ", PropertyZipCode: " + property_zip_code_value + ", PropertyZip4: " + property_zip4_value + ", PropertyUnitType: " + property_unit_type_value + ", PropertyUnitNumber: " + property_unit_number_value + ", PropertyState: " + property_unit_state_value + ", PropertyCityName: " + property_city_name_value if document["PropertyFullStreetAddress"] else "NA"

        if document["LC_CompanyTag1"] != 0:
            document["LC_Source"] = source
            document["LC_Borrower"] = document["LC_Borrower1FullName"]
            if document["LC_Borrower2FullName"] == "":
                document["LC_PartialLoanAmount"] = document[loan_amount_field]
            else:
                document["LC_PartialLoanAmount"] = document[loan_amount_field]//2
            to_insert_company.append(InsertOne(document.copy()))

        if document["LC_CompanyTag2"] != 0:
            document["LC_Source"] = source
            document["LC_Borrower"] = document["LC_Borrower2FullName"]
            if document["LC_Borrower1FullName"] == "":
                document["LC_PartialLoanAmount"] = document[loan_amount_field]
            else:
                document["LC_PartialLoanAmount"] = document[loan_amount_field]//2
            to_insert_company.append(InsertOne(document.copy()))


    if to_insert_company:
        collection_flattened_transaction.bulk_write(to_insert_company)



st = get_current_time()
print(f"Started with SAM collection")
flattening_collection(collection_filter_sam,"SAM")
et = get_current_time()
print(f"Done with SAM collection in {et-st} seconds")


st = get_current_time()
print(f"Started with DEED collection")
flattening_collection(collection_filter_deed,"DEED")
et = get_current_time()
print(f"Done with DEED collection in {et-st} seconds")





