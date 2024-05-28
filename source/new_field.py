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

def addNullField(operation, db, collection_name, new_field, name, method):
    """
    Insert new field in all documents in a specific collection from the database
    """
    if collection_name in db.list_collection_names():
        # Access the collection:
        collection = db[collection_name]
        # Find all documents before the update to retrieve the previous values
        previous_documents = collection.find({})
        
        updates_made = 0

        print(f"Adding field to all files in the {collection_name} collection.")
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
    else:
        print(f"There's no collection named {collection_name}.")


def addFieldFile(operation, db, collection_name, new_field_file, name, method):
    """
    Update multiple documents with information from a csv
    """
    if os.path.exists(new_field_file):
        # Import the update information
        update_data = pd.read_csv(new_field_file)

        # Get the fields:
        column_names = update_data.columns.to_list()
        field_to_match = column_names[0]  # The header from the first column will always be the field to match
        new_field = column_names[1]  # The header from the second column will always be the update field

        # Get the values from the columns
        values_to_match = update_data[field_to_match].values
        new_values = update_data[new_field].values

        print(f'There are {len(values_to_match)} objects with a filled new field:')

        # Access the collection:
        collection = db[collection_name]

        # Insert metadata about the update process
        process_id = log_functions.insertLog(db, name, method, operation, collection_name)

        # Prepare data for insertion into the files collection
        files_data = {
            "log_id": str(process_id),
            "operation": operation,
            field_to_match: values_to_match.tolist(),  # Convert numpy array to Python list
            new_field: new_values.tolist()  # Convert numpy array to Python list
        }

        # Access or create the files collection:
        files_collection = db["update_files"]

        # Insert the data into the files collection
        files_collection.insert_one(files_data)

        # For each row, use the update one function to modify the specific field stated in the file.
        updates_made = 0
        for value_to_match, new_value in zip(values_to_match, new_values):
            # Stable id of the object to be updated
            update_criteria = {field_to_match: value_to_match}

            # Find the document before the update to retrieve the previous value
            previous_document = collection.find_one(update_criteria)

            # Check if the document with the specified criteria exists in the collection
            if previous_document:
                # Update the log field in the JSON document
                updated_log = log_functions.updateLog(previous_document, process_id, operation, new_field,
                                                        "Non-existing", new_value)

                # Update the document with the new data
                result = collection.update_one(update_criteria,
                                                {"$set": {new_field: new_value, "log": updated_log}})
                
                # Print whether the document was updated or not
                if result.modified_count > 0:
                    updates_made += 1
                    print(f'Field {new_field} added successfully in the document with {list(update_criteria.keys())[0]}: {list(update_criteria.values())[0]}')
                    print('')
                else:
                    print(f'Field {new_field} in the document with {list(update_criteria.keys())[0]}: {list(update_criteria.values())[0]} remains unchanged.')
                    print('')
            else:
                print(f"The document you are searching for is not in the collection.")

        # Handle documents that are not inside the file. The new field will have an empty value.
        print("Adding the field (empty) to the rest of documents.")
        all_documents = collection.find() # Get all the documents
        unmatched_updates_made = 0 # Count the amount of updates.
        for doc in all_documents: 
            if doc[field_to_match] not in values_to_match: # If the document is not inside the file.
                update_criteria = {field_to_match: doc[field_to_match]} # Get a general update criteria to change the documents one by one.
                updated_log = log_functions.updateLog(doc, process_id, operation, new_field, "Non-existing", None) # Generate log.
                result = collection.update_one(update_criteria, {"$set": {new_field: None, "log": updated_log}}) # Add the new field.
                
                if result.modified_count > 0:
                    unmatched_updates_made += 1 # Count how many unmatched updates were made

        total_updates_made = updates_made + unmatched_updates_made

        # If no updates were made, remove the meta and files documents
        if total_updates_made == 0:
            files_collection.delete_one({"meta_id": str(process_id)})
            log_functions.deleteLog(db, str(process_id))
            print("No changes were made.")
        else:
            print("Update done!")
            print(f"There are {updates_made} documents with a filled new field.")
            print(f"And {unmatched_updates_made} documents with an empty new field.")

    else:
        print(f'{new_field_file} file does not exist.')