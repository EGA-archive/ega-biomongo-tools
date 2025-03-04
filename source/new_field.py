#!/usr/bin/env python

"""new_field.py  :  Generate a new field in a collection """

__author__ = "Marta Huertas"
__version__ = "0.1"
__maintainer__ = "Aldar Cabrelles"
__email__ = "aldar.cabrelles@crg.eu"
__status__ = "development"


# Import Packages
import json
from . import log_functions
import os
import pandas as pd
from pymongo import UpdateOne
import datetime

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
    Insert a new field with values from a CSV. Only the documents specified in the CSV are assigned their corresponding values,
    while all other documents in the collection receive a Null value.
    """

    if not os.path.exists(new_field_file):
        print(f'{new_field_file} file does not exist.')
        return

    print(f"{datetime.datetime.now()} : Running addFieldFile")

    # Import the update information
    update_data = pd.read_csv(new_field_file)
    print(f"{datetime.datetime.now()} : CSV read")
    
    # Get the fields:
    column_names = update_data.columns.to_list()
    if len(column_names) < 2:
        print("The CSV file must have at least two columns.")
        return

    field_to_match = column_names[0]  # The header from the first column will be the field to match
    new_field = column_names[1]  # The header from the second column will be the new field

    print(f"{datetime.datetime.now()} : Field to match ({field_to_match}), New field ({new_field})")
    
    # Get the values from the columns
    csv_values_map = dict(zip(update_data[field_to_match], update_data[new_field]))  # Store CSV data in a dictionary

    # Access the collection
    collection = db[collection_name]

    # Fetch all documents in the collection
    all_documents = collection.find()
    
    if not all_documents:
        print("No documents found in the collection.")
        return

    # Insert metadata about the update process
    print(f"{datetime.datetime.now()} : Creating log file")
    process_id = log_functions.insertLog(db, name, method, operation, collection_name)
    print(f"{datetime.datetime.now()} : Log file created")

    # Prepare bulk update operations
    bulk_updates = []
    print(f"{datetime.datetime.now()} : Preparing bulk updates")

    matched_documents = 0
    unmatched_documents = 0

    for doc in all_documents:
        stable_id = doc.get(field_to_match)  # Get the document's ID

        # Determine if the document is in the CSV file
        if stable_id in csv_values_map:
            new_value = csv_values_map[stable_id]  # Assign the CSV value (matched document)
            matched_documents + 1
        else:
            new_value = None  # Assign None for unmatched documents
            unmatched_documents + 1

        # Get the previous value of the new field (if it exists)
        previous_value = doc.get(new_field, "Non-existing")  # If field didn't exist, set as "Non-existing"

        # Log the update
        updated_log = log_functions.updateLog(doc, process_id, operation, new_field, previous_value, new_value)

        # Add the update operation to the batch
        bulk_updates.append(UpdateOne({"_id": doc["_id"]}, {"$set": {new_field: new_value, "log": updated_log}}))

    print(f"{datetime.datetime.now()} : Bulk updates ready")

    # Execute bulk updates
    if bulk_updates:
        result = collection.bulk_write(bulk_updates)
        updates_made = result.modified_count
    else:
        updates_made = 0

    print(f"{datetime.datetime.now()} : Bulk update finished")

    # If no updates were made, remove the log entry
    if updates_made == 0:
        log_functions.deleteLog(db, str(process_id))
        print("No changes were made.")
    else:
        print(f"Total documents updated: {updates_made}")
        print(f"There are {matched_documents} documents with a filled new field.")
        print(f"And {unmatched_documents} documents with an empty new field.")
