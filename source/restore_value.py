#!/usr/bin/env python

"""restore.py  :  Reset to previous version as saved in meta """

__author__ = "Marta Huertas"
__version__ = "0.1.2"
__maintainer__ = "Aldar Cabrelles"
__email__ = "aldar.cabrelles@crg.eu"
__status__ = "development"

from . import log_functions

def restoreOne(operation, db, collection_name, reset_criteria, log_id, name, method):
    """
    Reset a field in a document to a previous version using log.
    """
    # Access the collection:
    collection = db[collection_name]

    # Find the document
    document = collection.find_one(reset_criteria)

    if not document:
        print('The document you are searching for is not in the collection.')
        return

    # Insert metadata about the reset process in the meta collection
    process_id = log_functions.insertLog(db, name, method, operation, collection_name)
    if not process_id:
        print('Failed to create log for the reset process.')
        return

    # Retrieve the 'log' field
    log_entries = document.get('log', [])

    # Find the log entry to restore
    log_entry = next((entry for entry in log_entries if entry.get('log_id') == log_id), None)

    if not log_entry:
        log_functions.deleteLog(db, str(process_id))
        print(f'The log_id: {log_id} does not exist in the document.')
        return

    if 'update' not in log_entry.get('operation') and 'remove' not in log_entry.get('operation'):
        log_functions.deleteLog(db, str(process_id))
        print('Only update operations can be restored.')
        return

    # Get the field and value to restore
    update_field = log_entry.get('modified_field')
    new_value = log_entry.get('previous_value')
    previous_value = document.get(update_field)

    # Remove the log entry being restored
    updated_log_entries = [entry for entry in log_entries if entry.get('log_id') != log_id]

    # Add a new log entry for the restore operation
    new_log_entry = {
        "log_id": str(process_id),
        "operation": operation,
        "previous_log_id": log_id,
        "modified_field": update_field,
        "previous_value": previous_value,
        "restored_value": new_value,
    }

    updated_log_entries.insert(0, new_log_entry)

    # Update the document with the restored value and new log entry
    result = collection.update_one(reset_criteria, {"$set": {update_field: new_value, "log": updated_log_entries}})
    # Check the number of documents updated
    updates_made = result.modified_count

    if updates_made > 0:
        print(f'Document restored to version with id: {log_id}.')
    else:
        log_functions.deleteLog(db, str(process_id))
        print("No changes were made.")


def restoreAll(operation, db, collection_name, log_id, name, method):
    """
    Reset a field in all documents in the collection to a previous version using log_id.
    """
    # Access the collection:
    collection = db[collection_name]

    # Find all documents in the collection
    documents = collection.find()

    # Insert metadata about the reset process in the meta collection
    process_id = log_functions.insertLog(db, name, method, operation, collection_name)

    if not process_id:
        print('Failed to create log for the reset process.')
        return

    restored_documents = 0
    update_field = None

    for document in documents:
        # Retrieve the 'log' field
        log = document.get('log', [])
        # Find the update corresponding to the desired log_id
        for update in log:
            if update.get('log_id') == log_id:
                if 'update' in update.get('operation') or 'remove' in update.get('operation'):
                    # Get the field and value to restore
                    update_field = update.get('modified_field')
                    previous_value = document.get(update_field)
                    new_value = update.get('previous_value')

                    # Delete log instance for the specific log_id
                    updated_log_info = [entry for entry in log if entry.get('log_id') != log_id]

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

                    if result.modified_count > 0:
                        restored_documents += 1
                else:
                    print('Only update operations can be restored.')
                break  # Exit the loop once the correct log_id is found and processed
            else:
                print(f"The log id {log_id} does not exist in the document.")
    if restored_documents == 0:
        log_functions.deleteLog(db, str(process_id))
        print("No changes were made.")
    else:
        print(f'Field {update_field} restored successfully in {restored_documents} documents.')

