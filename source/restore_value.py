#!/usr/bin/env python

"""restore.py  :  Reset to previous version as saved in meta """

__author__ = "Marta Huertas"
__version__ = "0.1"
__maintainer__ = "Marta Huertas"
__email__ = "marta.huertas@crg.eu"
__status__ = "development"

from . import log_functions

def restoreOne(operation, db, collection_name, reset_criteria, log_id, name, method):
    """
    Reset field in a document to a previous version using log.
    """
    # Access the collection:
    collection = db[collection_name]

    # Find the document
    document = collection.find_one(reset_criteria)

    if document: # If the specific document we want to restore exists:
        # Insert metadata about the reset process in the meta collection
        process_id = log_functions.insertLog(db, name, method, operation, collection_name)

        # Retrieve the 'meta_info' field
        log = document.get('log', [])

        # Find the index of the desired version in the 'meta_info' array
        log_test = False # Test if the meta_id exists
        for update in log:
            if update.get('log_id') == log_id:
                log_test = True
                if 'update' in update.get('operation'):
                    # Get the field and value to restore
                    update_field = update.get('modified_field')
                    new_value = update.get('previous_value')
                    previous_value = document.get(update_field)

                    if log_test:
                        # Delete log instance for the specific log_id
                        updated_log_info = [update for update in log if update.get('log_id') != log_id]

                        # Define the new metadata to be added
                        new_log_info = {
                            "log_id": str(process_id),
                            "operation": operation,
                            "previous_log_id": log_id,
                            "modified_field": update_field, 
                            "previous_value": previous_value,
                            "restored_value": new_value,
                        }

                        # Merge the new metadata with the existing log_info
                        updated_log_info.insert(0, new_log_info)

                        # Update the document with the new data
                        result = collection.update_one(reset_criteria, {"$set": {update_field: new_value, "log": updated_log_info}})

                        print(f'Document restored to version with id: {log_id}.')
                    else:
                        print('Meta id not found in this document. No change made.')
                else: 
                    print('Insert operations cannot be restored.')
    else:
        print('The document you are searching for is not in the collection.')


def restoreAll(operation, db, collection_name, log_id, name, method):
    """
    Reset field in all documents in the collection to a previous version using log_id.
    """
    # Access the collection:
    collection = db[collection_name]

    # Find all documents in the collection
    documents = collection.find()

    # Insert metadata about the reset process in the meta collection
    process_id = log_functions.insertLog(db, name, method, operation, collection_name)

    restored_documents = 0
    for document in documents:

        # Retrieve the 'log' field
        log = document.get('log', [])
        # Find the update corresponding to the desired log_id
        log_test = False  # Test if the log_id exists in the document
        for update in log:
            if update.get('log_id') == log_id:
                log_test = True
                if log_test:
                    if 'update' in update.get('operation'):
                        # Get the field and value to restore
                        update_field = update.get('modified_field')
                        previous_value = document.get(update_field)
                        new_value = update.get('previous_value')

                        if log_test:
                            # Delete log instance for the specific log_id
                            updated_log_info = [update for update in log if update.get('log_id') != log_id]

                            # Define the new metadata to be added
                            new_log_info = {
                                "log_id": str(process_id),
                                "operation": operation,
                                "previous_log_id": log_id,
                                "modified_field": update_field, 
                                "previous_value": previous_value,
                                "restored_value": new_value,
                            }

                            # Merge the new metadata with the existing log_info
                            updated_log_info.insert(0, new_log_info)

                            # Update the document with the new data
                            result = collection.update_one(
                                {"_id": document["_id"]},
                                {"$set": {update_field: new_value, "log": updated_log_info}}
                            )

                            restored_documents += 1 
                        
                    else:
                        print(f'Only update operations can be restored.')
                break  # Exit the loop once the correct log_id is found and processed
    
    print(f'Field {update_field} restored successfully in {restored_documents} documents.')
