#!/usr/bin/env python

"""new_field.py  :  Generate a new field in a collection """

__author__ = "Marta Huertas"
__version__ = "0.1"
__maintainer__ = "Aldar Cabrelles"
__email__ = "aldar.cabrelles@crg.eu"
__status__ = "development"


# Import Packages
from . import log_functions
from pymongo import UpdateOne

def addNullField(operation, db, collection_name, new_field, name, method):
    """
    Insert a new field with a null value in all documents in a specific collection from the database.
    """
    if collection_name not in db.list_collection_names():
        print(f"There's no collection named {collection_name}.")
        return

    # Access the collection
    collection = db[collection_name]

    # Check if the field exists in any document in the collection
    if not collection.find_one({new_field: {"$exists": True}}):
        # Insert metadata about the update process in the collection log_details
        process_id = log_functions.insertLog(db, name, method, operation, collection_name)
        if not process_id:
            print("Failed to create log for the update process.")
            return

        updates_made = 0
        print(f"Adding field {new_field} to all documents in the {collection_name} collection.")

        # Find all documents to update
        previous_documents = collection.find({})

        # Prepare bulk update operations
        bulk_updates = []
        for previous_document in previous_documents:
            # Update the log field in the JSON document
            updated_log = log_functions.updateLog(previous_document, process_id, operation, new_field, "Non-existing", None)

            # Create the update operation
            bulk_updates.append(
                UpdateOne({"_id": previous_document["_id"]}, {"$set": {new_field: None, "log": updated_log}})
            )

        # Execute the bulk update operations
        if bulk_updates:
            result = collection.bulk_write(bulk_updates)
            updates_made = result.modified_count

        # If no updates were made, remove the log document
        if updates_made == 0:
            log_functions.deleteLog(db, str(process_id))
            print("No changes were made.")
        else:
            print(f"Field added successfully in {updates_made} documents.")
    else:
        print(f"The field {new_field} already exists in at least one document in the collection.")