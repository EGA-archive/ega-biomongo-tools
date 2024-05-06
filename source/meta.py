#!/usr/bin/env python

""" meta.py  :  Generate meta information about the process """

__author__ = "Marta Huertas"
__version__ = "0.1"
__maintainer__ = "Marta Huertas"
__email__ = "marta.huertas@crg.eu"
__status__ = "development"


# Import Packages
from datetime import datetime

def insertMeta(db, name, method, operation, collection_name):
    """
    Function to generate an entrance to the meta collection with information about the process.
    """
    # Insert metadata into the 'meta' collection
    meta_collection = db['meta']
    process_info = {
        # The id is created by MongoDB, no need to generate one.
        "name": name,
        "operation" : operation,
        "collection": collection_name,
        "date": datetime.now().strftime('%Y-%m-%dT%H:%M:%S+00:00'),
        "method": method
    }
    meta_result = meta_collection.insert_one(process_info)

    return meta_result.inserted_id

def updateMeta(previous_document, process_id, operation, update_field, previous_value):
    """
    Update already generated meta_info field
    """
    # Get the existing meta_info, or an empty list if it doesn't exist
    existing_meta_info = previous_document.get("meta_info", [])

    # Define the new metadata to be added
    new_meta_info = {
        "meta_id" : str(process_id),
        "operation": operation,
        "modified_field": update_field,
        "previous_value": previous_value
    }

    # Merge the new metadata with the existing meta_info
    existing_meta_info.insert(0, new_meta_info)

    return existing_meta_info

