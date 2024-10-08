#!/usr/bin/env python

""" meta.py  :  Generate meta information about the process """

__author__ = "Marta Huertas"
__version__ = "0.1"
__maintainer__ = "Aldar Cabrelles"
__email__ = "aldar.cabrelles@crg.eu"
__status__ = "development"


# Import Packages
from datetime import datetime
from bson.objectid import ObjectId

def insertLog(db, name, method, operation, collection_name):
    """
    Function to generate an entrance to the meta collection with information about the process.
    """
    # Insert metadata into the 'meta' collection
    log_collection = db['log_details']
    process_info = {
        # The id is created by MongoDB, no need to generate one.
        "name": name,
        "operation" : operation,
        "collection": collection_name,
        "method": method, 
        "date": datetime.now().strftime('%Y-%m-%dT%H:%M:%S+00:00')
    }
    log_result = log_collection.insert_one(process_info)

    return log_result.inserted_id

def updateLog(previous_document, process_id, operation, update_field, previous_value, new_value):
    """
    Update already generated log field
    """
    # NEW
    # If there's no previous document (meaning the log doesn't exist yet), start with an empty log
    if previous_document is None:
        existing_log = []
    else:
        # Get the existing log, or start with an empty list if it doesn't exist
        existing_log = previous_document.get("log", [])
    ## END NEW

    # Define the new metadata to be added
    new_log = {
        "log_id" : str(process_id),
        "operation": operation,
        "modified_field": update_field,
        "previous_value": previous_value,
        "new_value": new_value
    }

    # Merge the new metadata with the existing log
    existing_log.insert(0, new_log)

    return existing_log

def deleteLog(db, process_id):
    """
    Delete metadata document from the database based on process_id
    """
    log_collection = db["log_details"]  # Assuming "meta" is the name of your metadata collection
    result = log_collection.delete_one({"_id": ObjectId(process_id)})
    

