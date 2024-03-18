import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv
import os
import pyarrow as pa
import pyarrow.parquet as pq
import json

DB_NAME = "lead_compass"
# MONGO_CONNECTION_URL = os.getenv("MONGO_CONNECTION_URL")
client = MongoClient("mongodb://localhost:27017")

load_dotenv()
# Collection details
db = client[DB_NAME]
collection_name = "test_sam"
collection = db[collection_name]


# Define Parquet file path
parquet_file = "sam_transactions.parquet"

# Define batch size
batch_size = 1000

# Fetch documents from MongoDB collection in batches
cursor = collection.find({}, {"_id": 0})

# Initialize an empty list to store batches
batches = []

# Iterate over the MongoDB cursor
for i, doc in enumerate(cursor):
    # Exclude "_id" field from each document and convert to strings
    doc_str = {key: str(value) for key, value in doc.items()}
    batches.append(doc_str)

    # Check if the batch size is reached
    if i % batch_size == batch_size - 1:
        # Convert the list of batches to a DataFrame
        df = pd.DataFrame(batches)

        # Convert DataFrame to PyArrow Table
        table = pa.Table.from_pandas(df)

        # Write the PyArrow Table to Parquet
        if i == batch_size - 1:
            pq.write_table(table, parquet_file)
        else:
            # Append to existing Parquet file
            existing_table = pq.read_table(parquet_file)
            updated_table = pa.concat_tables([existing_table, table])
            pq.write_table(updated_table, parquet_file)

        # Clear the list for the next batch
        batches = []

        print(f"Batch {i + 1} dumped to {parquet_file} in Parquet format.")

# Close MongoDB connection
client.close()