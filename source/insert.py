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

# Insert one function
def insertOne(operation, db, collection_name, json_documents, name, method):
    """
    Insert one document in a specific collection from a database.
    The collection will be created if it doesn't exist.
    """
    if os.path.exists(json_documents): # Test if json file exists
        # Read the JSON file
        with open(json_documents) as f:
            documents = json.load(f)

        # Access the collection:
        collection = db[collection_name]
        
        print(f"Documents inserted into {collection_name} collection")

        # Check if a document with the same ID already exists
        existing_document = collection.find_one({'stable_id': documents["stable_id"]})

        if existing_document:
            print(f"A document with the same stable_id already exists in the {collection_name} collection.")
        else:
            # Insert the document into the collection
            result = collection.insert_one(documents)

            doc_id = result.inserted_id

            # Print the inserted document ID
            print("Inserted document ID:", result.inserted_id)

            if result:
                # Get the ObjectId of the inserted process document
                process_id = log_functions.insertLog(db, name, method, operation, collection_name)

                # Update inserted document with a reference to the log document and operation
                log = [
                    {
                    "log_id" : str(process_id),
                    "operation": operation
                    }
                ]
                # Merge the log with the existing document
                collection.update_one({"_id": doc_id}, {"$set": {"log": log}})
            else:
                print("Document could not be inserted.")
    else:
        print(f'{json_documents} file does not exist')

def insertMany(operation, db, collection_name, json_documents, name, method):
    """
    Insert multiple documents into a specific collection from a database.
    The collection will be created if it doesn't exist.
    """
    if os.path.exists(json_documents):  # Test if json file exists
        # Read the JSON file
        with open(json_documents) as f:
            documents = json.load(f)

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

        print(f"{len(existing_identifiers)} of your documents already exist in the {collection_name} collection.")

        # Insert only new documents into the collection
        if new_documents:
            # Get the ObjectId of the inserted process document
            process_id = log_functions.insertLog(db, name, method, operation, collection_name)

            if process_id:
                # Insert new documents
                result = collection.insert_many(new_documents)
                inserted_ids = result.inserted_ids

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

                # Print the number of inserted documents
                print(f"Number of inserted documents: {len(inserted_ids)}")
                print("Log information generated and added to the documents.")
            else:
                print("Log details were not generated.")
        else:
            print("No new documents to insert.")
    else:
        print(f'{json_documents} file does not exist')
