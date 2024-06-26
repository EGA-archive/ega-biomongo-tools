#!/usr/bin/env python

"""new_field.py  :  Generate a new field in a collection """

__author__ = "Marta Huertas"
__version__ = "0.1"
__maintainer__ = "Marta Huertas"
__email__ = "marta.huertas@crg.eu"
__status__ = "development"


# Import Packages
import json
from . import log_functions
import os
import pandas as pd
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


def addFieldFile(operation, db, collection_name, new_field_file, name, method):
    """
    Update multiple documents with information from a CSV file.
    """
    if not os.path.exists(new_field_file):
        print(f'{new_field_file} file does not exist.')
        return

    # Import the update information
    update_data = pd.read_csv(new_field_file)

    # Get the fields:
    column_names = update_data.columns.to_list()
    if len(column_names) < 2:
        print("The CSV file must have at least two columns.")
        return

    field_to_match = column_names[0]  # The header from the first column will be the field to match
    new_field = column_names[1]  # The header from the second column will be the update field

    # Get the values from the columns
    values_to_match = update_data[field_to_match].values
    new_values = update_data[new_field].values

    # Access the collection
    collection = db[collection_name]

    # Check if at least one of the values_to_match is in the collection
    if not collection.find_one({field_to_match: {"$in": list(values_to_match)}}):
        print("None of the objects are present in the collection.")
        return

    # Insert metadata about the update process
    process_id = log_functions.insertLog(db, name, method, operation, collection_name)

    # Prepare data for insertion into the files collection
    files_data = {
        "log_id": str(process_id),
        "operation": operation,
        field_to_match: values_to_match.tolist(),  # Convert numpy array to Python list
        new_field: new_values.tolist()  # Convert numpy array to Python list
    }

    # Access or create the files collection
    files_collection = db["update_files"]
    files_collection.insert_one(files_data)

    # Prepare bulk update operations
    bulk_updates = []
    for value_to_match, new_value in zip(values_to_match, new_values):
        update_criteria = {field_to_match: value_to_match}
        previous_document = collection.find_one(update_criteria)

        if previous_document:
            updated_log = log_functions.updateLog(previous_document, process_id, operation, new_field, "Non-existing", new_value)
            bulk_updates.append(
                UpdateOne(update_criteria, {"$set": {new_field: new_value, "log": updated_log}})
            )

    # Execute bulk updates for matched documents
    if bulk_updates:
        result = collection.bulk_write(bulk_updates)
        updates_made = result.modified_count
    else:
        updates_made = 0

    # Handle documents that are not inside the file
    all_documents = collection.find()
    unmatched_updates_made = 0
    unmatched_bulk_updates = []
    for doc in all_documents:
        if field_to_match not in doc or doc[field_to_match] not in values_to_match:
            update_criteria = {"_id": doc["_id"]}
            updated_log = log_functions.updateLog(doc, process_id, operation, new_field, "Non-existing", None)
            unmatched_bulk_updates.append(
                UpdateOne(update_criteria, {"$set": {new_field: None, "log": updated_log}})
            )

    # Execute bulk updates for unmatched documents
    if unmatched_bulk_updates:
        result = collection.bulk_write(unmatched_bulk_updates)
        unmatched_updates_made = result.modified_count

    total_updates_made = updates_made + unmatched_updates_made

    # If no updates were made, remove the log entry
    if total_updates_made == 0:
        files_collection.delete_one({"log_id": str(process_id)})
        log_functions.deleteLog(db, str(process_id))
        print("No changes were made.")
    else:
        print(f"There are {updates_made} documents with a filled new field.")
        print(f"And {unmatched_updates_made} documents with an empty new field.")