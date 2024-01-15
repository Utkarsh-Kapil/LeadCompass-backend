import subprocess
import time
from pymongo import MongoClient
from bson import ObjectId
import sys

if "--project_id" in sys.argv:
    project_id_index = sys.argv.index("--project_id")
    ProjectId = sys.argv[project_id_index + 1]

    print(f"Project ID: {ProjectId}")
else:
    print("Project ID not provided.")
    sys.exit(1)

def get_current_time():
    return time.time()


start = get_current_time()

starting_deed_table = "test_deed"
starting_mortgage_table = "test_sam"
filtered_sam = "test_filtered_sam"
filtered_deed = "test_filtered_deed"
flattened_transactions = "test_flattened_transactions"
final_flattened_transactions = "test_final_flattened_transactions"
filtered_borrowers = "test_filtered_borrowers"
project_id = ProjectId


client = MongoClient("mongodb://localhost:27017")
db = client["lead_compass"]
collection_flattened_transactions = db["test_flattened_transactions"]
collection_project = db["project"]

# Run the first script
st = get_current_time()
print("Start 1_adding_tags_to_sam_data script")
subprocess.call(["python", "scripts/adding_borrower_name.py", starting_deed_table, starting_mortgage_table,project_id])
et = get_current_time()
print(f"Total time taken by script 1_adding_tags_to_raw_data.py is {et-st} seconds")



# Run the second script
st = get_current_time()
print("Start 2_filtering_sam_and_deed script")
subprocess.call(["python", "scripts/filter_sam_deed.py", starting_deed_table, starting_mortgage_table,filtered_deed,filtered_sam,project_id])
et = get_current_time()
print(f"Total time taken by script 2_filtering_sam_and_deed.py is {et-st} seconds")


# Run the third script
st = get_current_time()
print("Start 3_flattening_tables script")
subprocess.call(
    ["python", "scripts/flattened_sam_deed.py", filtered_deed, filtered_sam,flattened_transactions,project_id])
et = get_current_time()
print(f"Total time taken by script 3_flattening_tables script.py is {et - st} seconds")


# Run the fourth script
st = get_current_time()
print("Start adding_tag_flattened_sam_deed ")
subprocess.call(
    ["python", "scripts/adding_tag_flattened_sam_deed.py", flattened_transactions,project_id])
et = get_current_time()
print(f"Total time taken by script 4_Start adding_tag_flattened_sam_deed is {et - st} seconds")

# Run the fifth script
st = get_current_time()
print("Start 5_final_flattened_transactions script")
subprocess.call(["python", "scripts/final_flattened.py", flattened_transactions,
               final_flattened_transactions,project_id])
et = get_current_time()
print(f"Total time taken by script 5_final_flattened_transactions.py is {et - st} seconds")


# Run the sixth script
st = get_current_time()
print("Start 6_filtered_borrowers script")
subprocess.call(["python", "scripts/filtered_borrowers.py", final_flattened_transactions, filtered_borrowers,project_id])
et = get_current_time()
print(f"Total time taken by script 6_filtered_borrowers.py is {et - st} seconds")


end = get_current_time()

print(f"Total time taken for whole script is {et - st} seconds")




last_10_year_transactions_mortgage = collection_flattened_transactions.count_documents({
        "LC_TransactionDateValidForCompany": "Y",
        "ProjectId": project_id
    })

residential_properties_transactions_mortgage = collection_flattened_transactions.count_documents({
    "LC_TransactionDateValidForCompany": "Y",
    "LC_PropertyResidentialStatus": 1,
    "ProjectId": project_id
})

collection_project.update_one(
        {"_id": ObjectId(project_id)},
        {
            "$set": {
                "last_10_year_transactions_mortgage": last_10_year_transactions_mortgage,
                "residential_properties_transactions_mortgage": residential_properties_transactions_mortgage,
                "status": "completed"
            }
        }
    )

print("project update completed")
