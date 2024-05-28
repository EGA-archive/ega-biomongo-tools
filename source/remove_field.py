#!/usr/bin/env python

"""rename.py  :  Change the name of a field in a specific collection. """

__author__ = "Marta Huertas"
__version__ = "0.1"
__maintainer__ = "Marta Huertas"
__email__ = "marta.huertas@crg.eu"
__status__ = "development"

from . import log_functions
from pymongo import UpdateOne


# CONSIDERATIONS:
# IT IS IMPORTANT TO KNOW THAT YOU WILL REMOVE THE FIELD FROM ALL THE DOCUMENTS IN A COLLECTION
def ask_user(prompt):
    """
    Ask the user a yes/no question and return True for 'yes' and False for 'no'.
    """
    while True:
        response = input(f"{prompt} (yes/no): ").strip().lower()
        if response in ['yes', 'y']:
            return True
        elif response in ['no', 'n']:
            return False
        else:
            print("Please answer 'yes' or 'no'.")

def removeField(operation, db, collection_name, field_to_remove, name, method):
    """
    Remove a specific field in all the documents of a collection.
    """
    # Confirm with the user before proceeding
    if not ask_user(f"Are you sure you want to remove the field '{field_to_remove}' from all documents in the {collection_name} collection?"):
        print("Operation canceled.")
        return

    # Access the collection
    collection = db[collection_name]

    # Check if the field exists in at least one document in the collection
    if collection.find_one({field_to_remove: {"$exists": True}}):
        # Insert metadata about the update process
        process_id = log_functions.insertLog(db, name, method, operation, collection_name)
        
        # Prepare a list of bulk update operations
        bulk_updates = []
        previous_documents = collection.find({field_to_remove: {"$exists": True}})
        
        for previous_document in previous_documents:
            # Get the previous value of the field
            previous_value = previous_document.get(field_to_remove)

            # Update the log field in the JSON document
            updated_log = log_functions.updateLog(previous_document, process_id, operation, field_to_remove, previous_value, "Non-existing")

            # Create the update operation
            bulk_updates.append(UpdateOne(
                {"_id": previous_document["_id"]},
                {"$unset": {field_to_remove: ""}, "$set": {"log": updated_log}}
            ))

        # Execute the bulk update operations
        if bulk_updates:
            result = collection.bulk_write(bulk_updates)
            updates_made = result.modified_count

            if updates_made == 0:
                log_functions.deleteLog(db, str(process_id))
                print("No changes were made.")
            else:
                print(f'Field {field_to_remove} removed from {updates_made} documents.')
        else:
            log_functions.deleteLog(db, str(process_id))
            print("No changes were made.")
    else:
        print(f"The field {field_to_remove} doesn't exist in any document in the collection.")

