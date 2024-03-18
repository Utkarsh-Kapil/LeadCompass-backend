import os
import time
import json
import sys
import pandas as pd
import networkx as nx
from datetime import datetime
from dotenv import load_dotenv
from pymongo import MongoClient
from bson import ObjectId
 
class JSONEncoder(json.JSONEncoder):  
    def default(self, o):  
        if isinstance(o, ObjectId):  
            return str(o)  
        return json.JSONEncoder.default(self, o)  
 
 
DB_NAME = "lead_compass"
 
def get_db_client():
    load_dotenv()
    MONGO_CONNECTION_URL = os.getenv("MONGO_CONNECTION_URL")
    client = MongoClient("mongodb://localhost:27017")
    return client
 
def get_db(client, db_name):
    return client[db_name]
 
def get_current_time():
    return time.time()
 
# Get the starting time
st = get_current_time()
 
with get_db_client() as client:
    db = get_db(client ,DB_NAME)
    # SOURCE = sys.argv[1]
    # TARGET = sys.argv[2]
    # PROJECT_ID = sys.argv[3]

    SOURCE = "test_filtered_lenders"
    TARGET = "test_lenders_cluster"
    PROJECT_ID = "65aa4aeedae0666f0977bc57"

    source_collection = db[SOURCE]
 
    projected_fields = {"LenderNameBeneficiary": 1, "LC_TotalLoanAmount": 1, "LC_NumberOfLoans": 1, "LC_LenderFullAddressJsonSet": 1, "LC_LatestTransactionDate": 1, "LC_TotalNumberOfPropertyTransactions": 1, "_id": 1, "LC_LenderFullAddressSet":1, "ProjectId": 1 }
    
    project_id = PROJECT_ID
    # Fetch all documents from the collection
    cursor = source_collection.find({"ProjectId":project_id,"LC_LenderFullAddressSet":{"$ne":"NA"}}, projected_fields)
    # cursor = source_collection.find({}, projected_fields)

    df = pd.DataFrame(list(cursor))
 
    unique_mail_addresses_list = []
    map = {}
 
    # Iterate through the rows of the dataframe
    for index, row in df.iterrows():
        address_list = row['LC_LenderFullAddressSet']
        address_list = [item for item in address_list if item is not None]
 
        for address in address_list:
            if address not in map:
                unique_mail_addresses_list.append(address)
                map[address] = {"LC_LendersList": [row['LenderNameBeneficiary']]}
            else:
                map[address]["LC_LendersList"].append(row['LenderNameBeneficiary'])
 
    print("Done creating address Lenders map")
 
    # Create a DSU object with number of unique mail addresses as parameter
    graph = nx.Graph()
    graph.add_nodes_from(unique_mail_addresses_list)
 
    # Iterate through the rows of the dataframe
    for index, row in df.iterrows():
        address_list_large = row['LC_LenderFullAddressSet']
        address_list_large = [item for item in address_list_large if item is not None]
 
        for address_nth in address_list_large[1:]:
            graph.add_edge(address_list_large[0], address_nth)
 
    # Use connected components to find clusters
    clusters = list(nx.connected_components(graph))
 
    print("Done graph clustering moving on other things")
    print(f"Total clusters created is {len(clusters)}")
 
    res_list = []
 
 
    print("Started created index for data Frame")
 
    df.set_index('LenderNameBeneficiary', inplace=True)  
 
    print("Created index for the dataframe for field LC_Lender")
  
    for i, cluster in enumerate(clusters):
        if i % 5000 == 0:
            print(i)
    
        Lenders_set = set()
        for address in cluster:  
            Lenders_set.update(map[address]["LC_LendersList"])  
            
        # df_filtered = df[df['LC_Lender'].isin(Lenders_set)]  
        df_filtered = df.loc[df.index.isin(Lenders_set)]  
        
        # Lenders_metadata = df_filtered[['LC_Lender', 'LC_TotalLoanAmount', 'LC_NumberOfLoans', 'LC_LenderFullAddressSet', 'LC_LenderFullAddressJsonSet', 'LC_LatestTransactionDate', 'LC_TotalNumberOfPropertyTransactions', '_id']].set_index('LC_Lender').to_dict('index')
        Lenders_metadata = df_filtered[['LC_TotalLoanAmount', 'LC_NumberOfLoans', 'LC_LenderFullAddressSet', 'LC_LenderFullAddressJsonSet', 'LC_LatestTransactionDate', 'LC_TotalNumberOfPropertyTransactions', '_id', 'ProjectId']].to_dict('index')
 
        Lender_dpid_counts = {}    
        for Lender, metadata in Lenders_metadata.items():    
            Lender_dpid_counts[Lender] = metadata["LC_TotalNumberOfPropertyTransactions"]    
    
        LC_ParentSponsor = max(Lender_dpid_counts, key=Lender_dpid_counts.get)  
    
        Lenders_metadata_list = [{"LC_Lender": k, **v} for k, v in Lenders_metadata.items()]  
    
        res_list.append({"LC_ParentSponsor": LC_ParentSponsor, "LC_LenderMetaData": Lenders_metadata_list, "LC_FilterUsed": "MaxPropertyTransaction", "LC_DateOfParentCreation": datetime.now().strftime('%Y%m%d'),"ProjectId": project_id })  
    
 
    print("Done creating list for final insertion")
 
    # with open('data.json', 'w') as f:  
    #     f.write(JSONEncoder().encode(res_list)) 
     
    target_collection = db[TARGET]
    target_collection.insert_many(res_list)
 
    print("Done dumping data into a json file")
 
    # Get the end time
    et = get_current_time()
 
    print(f"Total time  taken in the whole process is {et-st} seconds")