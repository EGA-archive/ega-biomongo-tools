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
    Generate a log document with information about the process inside the log_details collection.
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
    Generate/update log information inside of every document.
    """

    # If there's no previous document (meaning the log doesn't exist yet), start with an empty log
    existing_log = [] if previous_document is None else previous_document.get("log", [])

    # Normalize values to lists for proper comparison
    prev_list = previous_value if isinstance(previous_value, list) else [previous_value] if previous_value is not None and previous_value != "Non-existing" else []
    new_list = new_value if isinstance(new_value, list) else [new_value] if new_value is not None else []

    # Compute added and removed values
    added_values = list(set(new_list) - set(prev_list))
    removed_values = list(set(prev_list) - set(new_list))

    # Prepare log entry
    new_log = {
        "log_id": str(process_id),
        "operation": operation,
        "modified_field": update_field,
    }

    # Include changed values only if there's a difference
    if previous_value is not None and previous_value != "Non-existing":
        new_log["changed_values"] = {}

        if added_values:  
            new_log["changed_values"]["added"] = added_values  

        if removed_values:  
            new_log["changed_values"]["removed"] = removed_values  


    # Merge the new metadata with the existing log
    existing_log.insert(0, new_log)

    return existing_log

def deleteLog(db, process_id):
    """
    Delete the log document inside the log_details collection based on process_id.
    """
    log_collection = db["log_details"]  # Assuming "meta" is the name of your metadata collection
    result = log_collection.delete_one({"_id": ObjectId(process_id)})
    

