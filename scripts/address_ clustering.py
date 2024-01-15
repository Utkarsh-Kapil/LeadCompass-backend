import pymongo,os,ast,json,sys
import networkx as nx
from dotenv import load_dotenv
from pymongo import MongoClient
import numpy as np
import pandas as pd


DB_NAME = "lead_compass"

def get_db_client():
    load_dotenv()
    MONGO_CONNECTION_URL = os.getenv("MONGO_CONNECTION_URL")
    client = MongoClient("mongodb://localhost:27017")
    return client

def get_db(client, db_name):
    return client[db_name]

client = get_db_client()

db = get_db(client ,DB_NAME) 
# SOURCE = sys.argv[1]
# TARGET = sys.argv[2]
# PROJECT_ID = sys.argv[3]

SOURCE = "test_filtered_borrowers"
TARGET = "test_borrowers_cluster"
PROJECT_ID = "65a3ff7be0d06b46084ef135"

source_collection = db[SOURCE]

project_id = PROJECT_ID
# Fetch all documents from the collection
cursor = source_collection.find({"ProjectId":PROJECT_ID})
# cursor = source_collection.find({})

df = list(cursor)

unique_mail_addresses_list = []

map = {}

# Iterate through the rows of the dataframe
for row in df:
    # Convert the data frame stringified list to a list
    address_list = row['LC_BorrowerFullAddressSet']

    address_list = [item for item in address_list if item is not None]

    for address in address_list:
        if address not in map:
            unique_mail_addresses_list.append(address)
            map[address] = {"LC_BorrowersList": [row['LC_Borrower']]}
        else:
            map[address]["LC_BorrowersList"].append(row['LC_Borrower'])

# Create a DSU object with number of unique mail addresses as parameter
graph = nx.Graph()
graph.add_nodes_from(unique_mail_addresses_list)

# Iterate through the rows of the dataframe
for row in df:
    address_list_large = row['LC_BorrowerFullAddressSet']

    address_list_large = [item for item in address_list_large if item is not None]

    for address_nth in address_list_large[1:]:
        graph.add_edge(address_list_large[0], address_nth)

    for address_nth in address_list_large[1:]:
        graph.add_edge(address_list_large[0], address_nth)

# Use connected components to find clusters
clusters = list(nx.connected_components(graph))

res_list = []
df = pd.DataFrame(df)
for i, cluster in enumerate(clusters):
    borrowers_set = set()
    borrower_counts = {}
    transactions_list_extended = []
    for address in cluster:
        borrowers_set.update(map[address]["LC_BorrowersList"])


    for borrower in borrowers_set:
        df_filtered = df[df['LC_Borrower'] == borrower]
        borrower_counts[borrower] = len(df_filtered["LC_Transactions"].values[0])
        transactions_list = df_filtered['LC_Transactions'].values[0]  
        transactions_list_extended.extend(transactions_list)
        borrower_counts[borrower] = len(transactions_list)
    
    address_json_list = []

    for address in cluster:
        single_address = {}
        df_transactions_list_extended = pd.DataFrame(transactions_list_extended)
        # print(df_transactions_list_extended)
        # df_transactions_list_extended_filtered = df_transactions_list_extended[df_transactions_list_extended['LC_BorrowerFullAddress'] == address]

        # single_address["BorrowerMailFullStreetAddress"] = df_transactions_list_extended_filtered["BorrowerMailFullStreetAddress"].values[0]
        # single_address["BorrowerMailZipCode"] = df_transactions_list_extended_filtered["BorrowerMailZipCode"].values[0]
        # single_address["BorrowerMailZip4"] = df_transactions_list_extended_filtered["BorrowerMailZip4"].values[0]
        # single_address["BorrowerMailUnitType"] = df_transactions_list_extended_filtered["BorrowerMailUnitType"].values[0]
        # single_address["BorrowerMailUnitNumber"] = df_transactions_list_extended_filtered["BorrowerMailUnitNumber"].values[0]
        # single_address["BorrowerMailState"] = df_transactions_list_extended_filtered["BorrowerMailState"].values[0]
        # single_address["BorrowerMailCity"] = df_transactions_list_extended_filtered["BorrowerMailCity"].values[0]

        address_json_list.append(single_address)

    res_list.append({"LC_LeadBorrower": max(borrower_counts, key=borrower_counts.get), "LC_BorrowersList": list(borrowers_set), "LC_BorrowerFullAddressListJson":address_json_list, "LC_BorrowerFullAddressListString": list(cluster), "LC_TransactionsList": transactions_list_extended})

client.close()

client = get_db_client()

db = get_db(client ,DB_NAME) 
target_collection = db[TARGET]

target_collection.insert_many(res_list)

client.close()

print("Done inserting")