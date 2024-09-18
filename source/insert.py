#!/usr/bin/env python

"""insert.py  :  Insert document in the desired MongoDB and collection """

__author__ = "Marta Huertas"
__version__ = "0.1"
__maintainer__ = "Aldar Cabrelles"
__email__ = "aldar.cabrelles@crg.eu"
__status__ = "development"


# Import Packages
import json
import re
from pymongo import UpdateOne
from . import log_functions
import os

def insertDocuments(operation, db, collection_name, json_documents, name, method):
    """
    Insert one or multiple documents into a specific collection from a database.
    The collection will be created if it doesn't exist.
    Supports inserting from a single file or multiple files in a directory.
    If there is already a document in the collection that matches the stable_id of a document, the function does not insert the duplicate document into the collection.
    """

    total_inserted_documents = 0  # Counter for tracking total inserted documents
    chunk_size = 10000 # Establishing the maximum number of embedded documents that a file can have before being split

    if os.path.exists(json_documents):  # Test if JSON file exists
        # Check if the path is a file or a directory
        if os.path.isfile(json_documents):
            json_files = [json_documents]  # Single file, put it in a list
            print("There is 1 file to process.")
        elif os.path.isdir(json_documents):
            # List all JSON files in the directory
            json_files = [os.path.join(json_documents, f) for f in os.listdir(json_documents) if os.path.isfile(os.path.join(json_documents, f)) and f.endswith('.json')]
            json_files = sorted(json_files, key=lambda s: [int(text) if text.isdigit() else text.lower() for text in re.split('([0-9]+)', s)])
            print(f"There is/are {len(json_files)} file(s) to process.")

        # Access the collection
        collection = db[collection_name]
        print(f"Inserting file(s) into {collection_name} collection")     


        # Begin loop for each JSON file
        for json_file in json_files:
            print(f"Processing {json_file}")

            # Read the JSON file
            with open(json_file) as f:
                data = json.load(f)

            # Determine if data is a single document or a list of documents
            if isinstance(data, list):
                documents = data
            else:
                documents = [data]

            # Get the unique identifier for each document
            unique_identifiers = [doc['stable_id'] for doc in documents]

            # Find existing documents with the same identifiers
            existing_documents = collection.find({'stable_id': {'$in': unique_identifiers}})
            existing_identifiers = {doc['stable_id'] for doc in existing_documents}

            # Filter out documents that are already in the collection
            new_documents = [doc for doc in documents if doc['stable_id'] not in existing_identifiers]

            # Split the new documents into chunks
            if new_documents:
                # Insert documents in chunks
                for i in range(0, len(new_documents), chunk_size):
                    chunk = new_documents[i:i + chunk_size]

                    result = collection.insert_many(chunk)
                    inserted_ids = result.inserted_ids

                    # Track total inserted documents across files
                    total_inserted_documents += len(inserted_ids)

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

                            print(f"Number of documents already existing in the collection with the same stable_id: {len(existing_identifiers)}")
                            print(f"Inserted {len(inserted_ids)} new documents from chunk {i // chunk_size + 1} of {json_file}.")
                            print(f"Log information generated and added to the documents.")
                        else:
                            print("Log details were not generated.")
                    else:
                        print(f"No new documents to insert from chunk {i // chunk_size + 1} of {json_file}.")
            else:
                print(f"No new documents to insert from {json_file}.")

        # Print the total number of inserted documents at the end
        print(f"Total number of documents inserted: {total_inserted_documents}")

    else:
        print(f'{json_documents} file or directory does not exist.')