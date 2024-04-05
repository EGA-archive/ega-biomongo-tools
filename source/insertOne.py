#!/usr/bin/env python

"""insertDoc.py  :  Insert document in the desired MongoDB and collection """

__author__ = "Marta Huertas"
__version__ = "0.1"
__maintainer__ = "Marta Huertas"
__email__ = "marta.huertas@crg.eu"
__status__ = "development"


# Import Packages
import json

# Insert one function
def insert_one(db, collection_name, document):
    """
    Insert one document in a specific database
    """
    # Access the collection:
    collection = db[collection_name]

    # Insert the document into the collection
    result = collection.insert_one(document)

    # Print the inserted document ID
    print("Inserted document ID:", result.inserted_id)