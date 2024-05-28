#!/usr/bin/env python

"""insert.py  :  Insert document in the desired MongoDB and collection """

__author__ = "Marta Huertas"
__version__ = "0.1"
__maintainer__ = "Marta Huertas"
__email__ = "marta.huertas@crg.eu"
__status__ = "development"


# Import Packages
import json
from pymongo import UpdateOne
from . import log_functions
import os

def insertDocuments(operation, db, collection_name, json_documents, name, method):
    """
    Insert one or multiple documents into a specific collection from a database.
    The collection will be created if it doesn't exist.
    """
    if os.path.exists(json_documents):  # Test if JSON file exists
        # Read the JSON file
        with open(json_documents) as f:
            data = json.load(f)

        # Determine if data is a single document or a list of documents
        if isinstance(data, list):
            documents = data
        else:
            documents = [data]

        # Access the collection
        collection = db[collection_name]

        print(f"Inserting into {collection_name} collection")

        # Get the unique identifier for each document
        unique_identifiers = [doc['stable_id'] for doc in documents]

        # Find existing documents with the same identifiers
        existing_documents = collection.find({'stable_id': {'$in': unique_identifiers}})
        existing_identifiers = {doc['stable_id'] for doc in existing_documents}

        # Filter out documents that are already in the collection
        new_documents = [doc for doc in documents if doc['stable_id'] not in existing_identifiers]

        if len(documents) == 1 and not existing_identifiers:
            # Handle the single document insertion
            document = documents[0]
            result = collection.insert_one(document)
            inserted_ids = [result.inserted_id]
        else:

            # Insert only new documents into the collection
            if new_documents:
                # Insert new documents
                result = collection.insert_many(new_documents)
                inserted_ids = result.inserted_ids
            else:
                inserted_ids = []

        if inserted_ids:
            # Get the ObjectId of the inserted process document
            process_id = log_functions.insertLog(db, name, method, operation, collection_name)

            if process_id:
                # Prepare bulk update operations to add the log field to the inserted documents
                log_info = {
                    "log_id": str(process_id),
                    "operation": operation
                }
                bulk_updates = [
                    UpdateOne({"_id": doc_id}, {"$set": {"log": [log_info]}})
                    for doc_id in inserted_ids
                ]

                # Perform bulk update operations
                if bulk_updates:
                    collection.bulk_write(bulk_updates)

                print(f"Number of inserted documents: {len(inserted_ids)}")
                print(f"Number of documents already exising in the collection: {len(existing_identifiers)}")
                print("Log information generated and added to the documents.")
            else:
                print("Log details were not generated.")
        else:
            print("No new documents to insert.")
    else:
        print(f'{json_documents} file does not exist')