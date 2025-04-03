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
    Reset the value of a field (embedded or non-embedded) in a document to a previous version using the log_id.
    """
    # Access the collection:
    collection = db[collection_name]

    # Find the document
    previous_document = collection.find_one(reset_criteria)

    # Check if the document exists
    if not previous_document:
        print(f"The document you are searching for is not in the collection.")
        return

    # Insert metadata about the restore process in the meta collection
    process_id = log_functions.insertLog(db, name, method, operation, collection_name)

    # Retrieve the logs
    log_entries = previous_document.get('log', [])

    # Find the log entry to restore
    log_entry = next((entry for entry in log_entries if entry.get('log_id') == log_id), None)
    
    if not log_entry:
        log_functions.deleteLog(db, str(process_id))
        print(f'The log_id: {log_id} does not exist in the document.')
        return

    if 'update' not in log_entry.get('operation'):
        log_functions.deleteLog(db, str(process_id))
        print('Only update operations can be restored.')
        return

    # Get the field and value to restore
    modified_field = log_entry.get('modified_field')
    
    # Retrieve current value (handle embedded fields)
    current_value = previous_document
    for key in modified_field.split("."):
        current_value = current_value.get(key, None)
        if current_value is None:
            break
    
    current_value_list = current_value if isinstance(current_value, list) else [current_value] if current_value is not None else []

    print(f"The modified field: {modified_field}")

    looped_logs = 0
    restored_value_list = current_value_list

    # Loop through the log entries to compute the restored value
    for entry in log_entries:
        if entry.get('log_id') == log_id:
            break

        modified_field_entry = entry.get('modified_field')
        
        if modified_field == modified_field_entry:
            looped_logs += 1
            print(f"Processing log entry {looped_logs}")

            added_value_list = entry.get('changed_values', {}).get('added', [])
            removed_value_list = entry.get('changed_values', {}).get('removed', [])

            print(f"Current value from this log entry: {restored_value_list}")
            print(f"Added values: {added_value_list}")
            print(f"Removed values: {removed_value_list}")

            # Compute the previous value (restored_value)
            restored_value_list = [x for x in restored_value_list if x not in added_value_list] + removed_value_list
            print(f"Restored value after this log entry: {restored_value_list}")

    # Convert single-element lists to a normal string
    restored_value_list = restored_value_list[0] if len(restored_value_list) == 1 else restored_value_list

    # Add a new log entry for the restore operation
    new_log_entry = {
        "log_id": str(process_id),
        "operation": operation,
        "modified_field": modified_field,
        "restored_value": restored_value_list,
    }
    log_entries.insert(0, new_log_entry)

    # Perform update
    result = collection.update_one(reset_criteria, {"$set": {modified_field: restored_value_list, "log": log_entries}})

    if result.modified_count > 0:
        print(f'Document with stable_id {reset_criteria.get("stable_id")} successfully restored to the value at log {log_id}')
    else:
        print('No changes were made.')

def restoreAll(operation, db, collection_name, log_id, name, method):
    """
    Reset a field (embedded or non-embedded) in all documents in the collection to a previous version using log_id.
    """
    # Access the collection:
    collection = db[collection_name]

    # Retrieve all documents in the collection
    documents = collection.find()

    # Insert metadata about the restore process in the meta collection
    process_id = log_functions.insertLog(db, name, method, operation, collection_name)
    if not process_id:
        print('Failed to create log for the restore process.')
        return

    restored_documents = 0
    
    for document in documents:
        log_entries = document.get('log', [])
        
        # Find the log entry to restore
        log_entry = next((entry for entry in log_entries if entry.get('log_id') == log_id), None)
        
        if not log_entry:
            print(f"The log_id {log_id} does not exist in the document with _id {document['_id']}")
            continue

        if 'update' not in log_entry.get('operation'):
            print(f"Only update operations can be restored in document with _id {document['_id']}")
            continue

        # Get the field and value to restore
        modified_field = log_entry.get('modified_field')

        # Retrieve current value (handle embedded fields)
        current_value = document
        for key in modified_field.split("."):
            current_value = current_value.get(key, None)
            if current_value is None:
                break
        
        current_value_list = current_value if isinstance(current_value, list) else [current_value] if current_value is not None else []
        
        looped_logs = 0
        restored_value_list = current_value_list

        # Loop through log entries to compute the restored value
        for entry in log_entries:
            if entry.get('log_id') == log_id:
                break

            if entry.get('modified_field') == modified_field:
                looped_logs += 1
                added_value_list = entry.get('changed_values', {}).get('added', [])
                removed_value_list = entry.get('changed_values', {}).get('removed', [])

                # Compute the restored value
                restored_value_list = [x for x in restored_value_list if x not in added_value_list] + removed_value_list
        
        # Convert single-element lists to a normal string
        restored_value_list = restored_value_list[0] if len(restored_value_list) == 1 else restored_value_list

        # Add a new log entry for the restore operation
        new_log_entry = {
            "log_id": str(process_id),
            "operation": operation,
            "modified_field": modified_field,
            "restored_value": restored_value_list,
        }
        log_entries.insert(0, new_log_entry)

        # Update the document
        result = collection.update_one(
            {"_id": document["_id"]},
            {"$set": {modified_field: restored_value_list, "log": log_entries}}
        )

        if result.modified_count > 0:
            restored_documents += 1
    
    if restored_documents == 0:
        log_functions.deleteLog(db, str(process_id))
        print("No changes were made.")
    else:
        print(f'Field {modified_field} restored successfully to the values at log {log_id} in {restored_documents} documents.')

