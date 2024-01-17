from pymongo import MongoClient
import os,sys
import time
from dotenv import load_dotenv, find_dotenv

_ = load_dotenv(find_dotenv())
DB_NAME = "lead_compass"

COLLECTION_DEED = sys.argv[1]
COLLECTION_SAM = sys.argv[2]
COLLECTION_FILTERED_DEED = sys.argv[3]
COLLECTION_FILTERED_SAM= sys.argv[4]
PROJECT_ID = sys.argv[5]

print(COLLECTION_DEED,COLLECTION_SAM,COLLECTION_FILTERED_DEED,COLLECTION_FILTERED_SAM,PROJECT_ID)

print(type(PROJECT_ID))
# COLLECTION_DEED = "test_deed"
# COLLECTION_SAM = "test_sam"
# COLLECTION_FILTERED_SAM= "test_filtered_sam"
# COLLECTION_FILTERED_DEED = "test_filtered_deed"
# PROJECT_ID = "65a79a690d174f3078f369da"

# MONGO_CONNECTION_URL = os.getenv("mongodb://localhost:27017")
def get_current_time():
    return time.time()

client = MongoClient("mongodb://localhost:27017")

# Accessing the database
db = client[DB_NAME]

# Access the raw collection DEED
collection_deed = db[COLLECTION_DEED]
# Access the raw collection SAM
collection_sam = db[COLLECTION_SAM]

collection_filtered_deed = db[COLLECTION_FILTERED_DEED]
collection_filtered_sam = db[COLLECTION_FILTERED_SAM]
project_id = PROJECT_ID


st = get_current_time()

filter = {
    "$and": [
        {"LC_TransactionDateValidForCompany": "Y"},
        {"LC_PropertyResidentialStatus": 1},
        {"LC_CompanyTag1": {"$ne":0}},
        {"ProjectId": project_id}
    ]
}

# Dealing with DEED collection
total_documents = collection_deed.count_documents(filter)

print("Documents filtering Started for deed:" )
print(total_documents)

batch_size = 1000

skip = 0
while skip < total_documents:
    print(skip)
    # Fetch documents in batches
    cursor = collection_deed.find(filter).skip(skip).limit(batch_size)

    # Create a list to store the documents in the batch
    batch_documents = list(cursor)

    for document in batch_documents:
        del document['_id']

    # Insert the batch of documents into the target collection
    collection_filtered_deed.insert_many(batch_documents)

    # Update the skip value for the next batch
    skip += batch_size

et = get_current_time()

print(f"total time taken in filtering deed {et-st} seconds")


# Dealing with SAM collection
total_documents = collection_sam.count_documents(filter)

print("Documents filtering Started for sam: ")
print(total_documents)

batch_size = 1000

skip = 0
while skip < total_documents:
    print(skip)
    # Fetch documents in batches
    cursor = collection_sam.find(filter).skip(skip).limit(batch_size)

    # Create a list to store the documents in the batch
    batch_documents = list(cursor)

    for document in batch_documents:
        del document['_id']

    # Insert the batch of documents into the target collection
    collection_filtered_sam.insert_many(batch_documents)

    # Update the skip value for the next batch
    skip += batch_size

et = get_current_time()

print(f"total time taken in filtering sam {et-st} seconds")

client.close()



# def filtering_collection(collection,source):
#     filter = {
#         "$and": [
#             {"LC_TransactionDateValidForCompany": "Y"},
#             {"LC_PropertyResidentialStatus": 1},
#             {"LC_CompanyTag1": {"$ne":0}},
#             {"ProjectId": project_id}
#         ]
#     }
#     total_documents = collection.count_documents(filter)

#     print(f"Documents filtering Started for {source}: ")
#     print(total_documents)

#     batch_size = 1000

#     skip = 0
#     while skip < total_documents:
#         print(skip)
#         # Fetch documents in batches
#         cursor = collection.find(filter).skip(skip).limit(batch_size)

#         # Create a list to store the documents in the batch
#         batch_documents = list(cursor)

#         for document in batch_documents:
#             del document['_id']

#         # Insert the batch of documents into the target collection
#         collection_filtered_sam.insert_many(batch_documents)

#         # Update the skip value for the next batch
#         skip += batch_size


# st = get_current_time()
# print("Started updating MORTGAGE collection")
# filtering_collection(collection_sam,"SAM")
# et = get_current_time()
# print("Done updating MORTGAGE collection")
# print(f"Total time taken in updating mortgage collection is {et-st} seconds")



# st = get_current_time()
# print("Started updating DEED collection")
# filtering_collection(collection_deed,"DEED")
# et = get_current_time()
# print("Done updating DEED collection")
# print(f"Total time taken in updating DEED collection is {et-st} seconds")