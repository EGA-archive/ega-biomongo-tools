#!/usr/bin/env python

"""insert.py  :  Insert document in the desired MongoDB and collection """

__author__ = "Marta Huertas"
__version__ = "0.1"
__maintainer__ = "Marta Huertas"
__email__ = "marta.huertas@crg.eu"
__status__ = "development"


# Import Packages
import json

# Insert one function
def insertOne(db, collection_name, json_documents):
    """
    Insert one document in a specific collection from a database.
    The collection will be created if it doesn't exist.
    """
    # Read the JSON file
    with open(json_documents) as f:
        documents = json.load(f)

    # Access the collection:
    collection = db[collection_name]
    
    print(f"Documents inserted into {collection_name} collection")

    # Insert the document into the collection
    result = collection.insert_one(documents)

    # Print the inserted document ID
    print("Inserted document ID:", result.inserted_id)

# Insert one function
def insertMany(db, collection_name, json_documents):
    """
    Insert one document in a specific collection from a database.
    The collection will be created if it doesn't exist.
    """
    # Read the JSON file
    with open(json_documents) as f:
        documents = json.load(f)

    # Access the collection:
    collection = db[collection_name]

    print(f"Documents inserted into {collection_name} collection")

    # Insert the document into the collection
    result = collection.insert_many(documents)

    # Print the inserted document ID
    print(f"Number of inserted documents: {len(result.inserted_ids)}")