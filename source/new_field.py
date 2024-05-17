#!/usr/bin/env python

"""insert.py  :  Insert document in the desired MongoDB and collection """

__author__ = "Marta Huertas"
__version__ = "0.1"
__maintainer__ = "Marta Huertas"
__email__ = "marta.huertas@crg.eu"
__status__ = "development"


# Import Packages
import json
from . import log_functions

def addNullField(operation, db, collection_name, new_field, name, method):
    """
    Insert new field in all documents in a specific collection from the database
    """
    # Access the collection:
    collection = db[collection_name]

    # Find all documents before the update to retrieve the previous values
    previous_documents = collection.find({})
    
    updates_made = 0
    # Check if the field exists in at least one document in the collection, if it doesn't create the field in all documents
    if collection.find_one({new_field: {"$exists": False}}):
        # Insert metadata about the update process in the collection log_details
        process_id = log_functions.insertLog(db, name, method, operation, collection_name)
        
        # Iterate over each document to add the new field and update the log.
        for previous_document in previous_documents:
            # Update the log field in the JSON document
            updated_log = log_functions.updateLog(previous_document, process_id, operation, new_field, "Non-existing", None)

            # Update the document with the new metadata
            result = collection.update_one({"_id": previous_document["_id"]}, {"$set": {new_field: None, "log": updated_log}})

            # Print whether the document was updated or not
            if result.modified_count > 0:
                updates_made += 1
        # If no updates were made, remove the meta and files documents
        if updates_made == 0:
            log_functions.deleteLog(db, str(process_id))
            print("No changes were made.")
        else:
            print(f'Field {new_field} added successfully in all the documents.')
    else:
        print(f"The field {new_field} exists in at least one document in the collection.")
