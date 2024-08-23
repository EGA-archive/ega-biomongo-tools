#!/usr/bin/env python

"""rename.py  :  Change the name of a field in a specific collection. """

__author__ = "Marta Huertas"
__version__ = "0.1"
__maintainer__ = "Aldar Cabrelles"
__email__ = "aldar.cabrelles@crg.eu"
__status__ = "development"

from . import log_functions
from pymongo import UpdateOne

# CONSIDERATIONS:
# If the field ro rename does not exist, the function does nothing.
# The function does not work on embedded documents in arrays.

def renameField(operation, db, collection_name, field_name, new_field_name, name, method):
    """
    Change the name of a specified field.
    """
    # Access the collection
    collection = db[collection_name]

    # Check if the field exists in at least one document in the collection
    if not collection.find_one({field_name: {"$exists": True}}):
        print(f"The field {field_name} doesn't exist in any document in the collection.")
        return
    
    # Insert metadata about the update process
    process_id = log_functions.insertLog(db, name, method, operation, collection_name)

    # Prepare a list of bulk update operations
    bulk_updates = []
    previous_documents = collection.find({field_name: {"$exists": True}})
    
    for previous_document in previous_documents:
        # Update the log field in the JSON document
        updated_log = log_functions.updateLog(previous_document, process_id, operation, "field", field_name, new_field_name)

        # Create the rename and set operation
        update_query = {
            "$rename": {field_name: new_field_name},
            "$set": {"log": updated_log}
        }

        # Add the update operation to the bulk operations list
        bulk_updates.append(UpdateOne({"_id": previous_document["_id"]}, update_query))

    # Execute the bulk update operations
    if bulk_updates:
        result = collection.bulk_write(bulk_updates)
        updates_made = result.modified_count

        if updates_made == 0:
            log_functions.deleteLog(db, str(process_id))
            print("No changes were made.")
        else:
            print(f'Field {field_name} renamed to {new_field_name} successfully in {updates_made} documents.')
    else:
        log_functions.deleteLog(db, str(process_id))
        print("No changes were made.")



