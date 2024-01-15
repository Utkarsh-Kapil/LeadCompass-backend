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

# COLLECTION_DEED = "test_deed"
# COLLECTION_MORTGAGE = "test_sam"
# PROJECT_ID = 1

MONGO_CONNECTION_URL = os.getenv("mongodb://localhost:27017")

# Function to get current time
def get_current_time():
    return time.time()

# Connect to MongoDB
client = MongoClient(MONGO_CONNECTION_URL)

# Accessing the database
db = client[DB_NAME]

# Access the raw collection DEED
collection_deed = db[COLLECTION_DEED]
# Access the raw collection SAM
collection_mortgage = db[COLLECTION_MORTGAGE]

# Fields to create full name tag for deed table
# fields_to_create_full_name_deed = ["Seller1FirstName&MiddleName","Seller1LastNameOrCorporation","Seller2FirstName&MiddleName","Seller2LastNameOrCorporation","Buyer1FirstName&MiddleName","Buyer1LastNameOrCorporation","Buyer2FirstName&MiddleName","Buyer2LastNameOrCorporation"]
fields_to_create_full_name_deed = ["Buyer1FirstName&MiddleName","Buyer1LastNameOrCorporation","Buyer2FirstName&MiddleName","Buyer2LastNameOrCorporation"]

# Fields to create full name tag for sam table
fields_to_create_full_name_mortgage = ["Borrower1FirstNameMiddleName","Borrower1LastNameOeCorporationName","Borrower2FirstNameMiddleName","Borrower2LastNameOrCorporationName"]

# Words to be searched to classify borrower as institutional
company_name_words = set(["TRUST", "INC", "LLP", "LP","LLC","FUND","INVESTMENTS","CO","PARTNERSHIP","CORP","PARTNERS","LTD","ASSOCIATES","PLC","PC","PLLC"])

# Codes that means that buyer/borrower is commercial in nature
company_codes = set(["AB","AC","AD","AE","AG","AR","BU","CN","CO","DB","ES","EX","FL","FM","FR","GN","GP","GV","ID","IL","ME","PA","PR","PT","RL","RT","SL","SP","ST","TR","TS","TT"])

# Fields on which we had to look for words search for "TRUST", etc.
fields_for_names_to_check_deed = ["Buyer1FirstName&MiddleName","Buyer1LastNameOrCorporation","Buyer2FirstName&MiddleName","Buyer2LastNameOrCorporation"]
fields_for_names_to_check_mortgage = ["Borrower1FirstNameMiddleName","Borrower1LastNameOeCorporationName","Borrower2FirstNameMiddleName","Borrower2LastNameOrCorporationName"]

fields_for_company_codes_deed = ["Buyer1IDCode","Buyer2IDCode"]
fields_for_company_codes_mortgage = ["Borrower1IDCode","Borrower2IDCode"]

property_use_code_list = ["APT", "COM", "COP", "EXE", "CND", "IMP", "LAN", "MFD", "MIX", "NEW", "PUD", "SFR", "RES", "TWN"]



# Getting full name from first and last name
def get_full_name(first_name,last_name):
    full_name = ""

    if first_name and last_name:
        full_name = first_name + " " + last_name
    else:
        full_name = (first_name if first_name else "") + (last_name if last_name else "")
    
    return full_name



# Finding company tag both code provided regex found and their or company due to
# 0 -> Not Company
# 1 -> Company due to Regex Search only
# 2 -> Company due to Code description only
# 3 -> Company due to Both Regex and Code description

# LC_CompanyTag1 -> Specifies if borrower/buyer is company
# LC_CompanyCodeGiven1 -> Specifies if it's company due to this code
# LC_CompanyRegex1 -> Specifies if it's a company due to regex serach of the word
# LC_CompanyTagMatching2 -> Specifies if borrower/buyer is company
# LC_CompanyCodeGiven2 -> Specifies if it's company due to this code
# LC_CompanyRegexMatching2 -> Specifies if it's a company due to regex serach of the word

def get_company_tags(document,source):
    company_tag_cal = ""
    company_tag_given = ""

    LC_CompanyTag1 = 0
    LC_CompanyTag2 = 0

    LC_CompanyCodeGiven1 = ""
    LC_CompanyCodeGiven2 = ""

    LC_CompanyRegexMatching1 = ""
    LC_CompanyRegexMatching2 = ""

    # SAM is hardcoded avoid this
    field_to_check = fields_for_names_to_check_mortgage if source == "SAM" else fields_for_names_to_check_deed

    company_codes_fields = fields_for_company_codes_mortgage if source == "SAM" else fields_for_company_codes_deed

    for field in field_to_check:
        if document[field]:
            for term in company_name_words:
                if term in document[field].upper().split(" "):
                    company_tag_cal += term
                    break
            company_tag_cal += "|"
        else:
            company_tag_cal += "|"

    company_tag_cal = company_tag_cal[:-1]

    splitting_the_calculated_tags = company_tag_cal.split("|")

    LC_CompanyRegexMatching1 = splitting_the_calculated_tags[1] if splitting_the_calculated_tags[1] else splitting_the_calculated_tags[0]
    LC_CompanyRegexMatching2 = splitting_the_calculated_tags[3] if splitting_the_calculated_tags[3] else splitting_the_calculated_tags[2]
    

    for field in company_codes_fields:
        if (document[field]) and (document[field].strip() in company_codes):
            company_tag_given += document[field] + "|"
        else:
            company_tag_given += "|"
    
    company_tag_given = company_tag_given[:-1]

    splitting_the_given_tags = company_tag_given.split("|")

    LC_CompanyCodeGiven1 = splitting_the_given_tags[0]
    LC_CompanyCodeGiven2 = splitting_the_given_tags[1]

    company_tags = {}

    company_tags["LC_CompanyRegexMatching1"] = LC_CompanyRegexMatching1
    company_tags["LC_CompanyRegexMatching2"] = LC_CompanyRegexMatching2
    company_tags["LC_CompanyCodeGiven1"] = LC_CompanyCodeGiven1
    company_tags["LC_CompanyCodeGiven2"] = LC_CompanyCodeGiven2

    if LC_CompanyRegexMatching1 == "" and LC_CompanyCodeGiven1 == "":
        LC_CompanyTag1 = 0
    elif LC_CompanyRegexMatching1 == "":
        # Company due to code given only
        LC_CompanyTag1 = 2
    elif LC_CompanyCodeGiven1 == "":
        # Company due to regex search only
        LC_CompanyTag1 = 1
    else:
        # Company due to both specifications
        LC_CompanyTag1 = 3

    company_tags["LC_CompanyTag1"] = LC_CompanyTag1

    if LC_CompanyRegexMatching2 == "" and LC_CompanyCodeGiven2 == "":
        LC_CompanyTag2 = 0
    elif LC_CompanyRegexMatching2 == "":
        # Company due to code given only
        LC_CompanyTag2 = 2
    elif LC_CompanyCodeGiven2 == "":
        # Company due to regex search only
        LC_CompanyTag2 = 1
    else:
        # Company due to both specifications
        LC_CompanyTag2 = 3

    company_tags["LC_CompanyTag2"] = LC_CompanyTag2


    return company_tags


def get_residential_status_for_property(document):
    residential_tag = 0
    if (document["ResidentialIndicator"] and document["ResidentialIndicator"] == 1) or (document["PropertyUseCode"] and document["PropertyUseCode"].strip() in property_use_code_list):
        residential_tag = 1

    return residential_tag


# Set batch size based on your system's memory constraints
batch_size = 1000


def update_batch(update_batch,collection):
    bulk_operations = []

    for update_query, update_data in update_batch:
        bulk_operations.append(UpdateOne(update_query, update_data))
    collection.bulk_write(bulk_operations)


def update_data_in_batches(collection,source):
    cursor = collection.find({"ProjectId":PROJECT_ID}, no_cursor_timeout=True)
    # cursor = collection.find({}, no_cursor_timeout=True)

    
    batch = []
    count = 0

    for document in cursor:
        count += 1
        if count % batch_size == 0:
            update_batch(batch,collection)
            print(f"Reached upto {count} rows")
            batch = []

        tags = get_company_tags(document,source)
        tags["LC_PropertyResidentialStatus"] = get_residential_status_for_property(document)
        tags["LC_Borrower1FullName"] = get_full_name(document[fields_to_create_full_name_mortgage[0]],document[fields_to_create_full_name_mortgage[1]]) if source == "SAM" else get_full_name(document[fields_to_create_full_name_deed[0]],document[fields_to_create_full_name_deed[1]])
        tags["LC_Borrower2FullName"] = get_full_name(document[fields_to_create_full_name_mortgage[2]],document[fields_to_create_full_name_mortgage[3]]) if source == "SAM" else get_full_name(document[fields_to_create_full_name_deed[2]],document[fields_to_create_full_name_deed[3]])

        tags["LC_TransactionDateValidForCompany"] = "Y" if (document["OriginalDateOfContract"]) and (document["OriginalDateOfContract"] > 20130000) else "N"
        tags["LC_TransactionDateValidForIndividual"] = "Y" if (document["OriginalDateOfContract"]) and (document["OriginalDateOfContract"] > 20230000) else "N"

        update_query = {
            "_id": document["_id"]
        }
        update_data = {
            "$set": tags
        }

        batch.append((update_query, update_data))

    if batch:
        update_batch(batch,collection)




st = get_current_time()
print("Started updating MORTGAGE collection")
update_data_in_batches(collection_mortgage,"SAM")
et = get_current_time()
print("Done updating MORTGAGE collection")
print(f"Total time taken in updating mortgage collection is {et-st} seconds")



st = get_current_time()
print("Started updating DEED collection")
update_data_in_batches(collection_deed,"DEED")
et = get_current_time()
print("Done updating DEED collection")
print(f"Total time taken in updating DEED collection is {et-st} seconds")