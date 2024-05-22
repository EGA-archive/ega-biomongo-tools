#!/usr/bin/env python

"""update.py  :  Update specific or many documents in the desired MongoDB and collection """

__author__ = "Marta Huertas"
__version__ = "0.1"
__maintainer__ = "Marta Huertas"
__email__ = "marta.huertas@crg.eu"
__status__ = "development"

from . import log_functions
import pandas as pd
import os

# Update one function
def updateOne(operation, db, collection_name, update_criteria, update_field, new_value, name, method):
    """
    Update one document in a specific collection from the database
    """
    # Access the collection:
    collection = db[collection_name]
    
    # Find the document before the update to retrieve the previous value
    previous_document = collection.find_one(update_criteria)
    
    # Check if the document with the specified criteria exists in the collection
    if previous_document:
        # Check if the field exists in the document
        if update_field in previous_document:
            updates_made = 0
            # Get the previous value of the field
            previous_value = previous_document.get(update_field)

            # Insert metadata about the update process in the meta collection
            process_id = log_functions.insertLog(db, name, method, operation, collection_name)

            # Update the log field in the JSON document
            updated_log = log_functions.updateLog(previous_document, process_id, operation, update_field, previous_value, new_value)

            # Update the document with the new data
            result = collection.update_one(update_criteria, {"$set": {update_field: new_value, "log": updated_log}})
            
            # Print whether the document was updated or not
            if result.modified_count > 0:
                updates_made += 1
                print(f'Field {update_field} updated successfully in the document with {list(update_criteria.keys())[0]}: {list(update_criteria.values())[0]}')
                print('')
            # If no updates were made, remove the meta and files documents
            elif updates_made == 0:
                log_functions.deleteLog(db, str(process_id))
                print("No changes were made.")
        else:
            print(f"The field {update_field} doesn't exist in the document.")
    else:
        print(f"The document you are searching for is not in the collection.") 

# Update all function
def updateAll(operation, db, collection_name, update_field, new_value, name, method):
    """
    Update all documents in a specific collection from the database
    """
    # Access the collection:
    collection = db[collection_name]

    # Find all documents before the update to retrieve the previous values
    previous_documents = collection.find({})
    
    updates_made = 0
    # Check if the field exists in at least one document in the collection
    if collection.find_one({update_field: {"$exists": True}}):
        # Insert metadata about the update process
        process_id = log_functions.insertLog(db, name, method, operation, collection_name)
        
        # Iterate over each document to retrieve previous values and update with metadata
        for previous_document in previous_documents:
            # Check if the field exists in the document
            if update_field in previous_document:
                # Get the previous value of the field
                previous_value = previous_document.get(update_field)

                # Update the log field in the JSON document
                updated_log = log_functions.updateLog(previous_document, process_id, operation, update_field, previous_value, new_value)

                # Update the document with the new metadata
                result = collection.update_one({"_id": previous_document["_id"]}, {"$set": {update_field: new_value, "log": updated_log}})

                # Print whether the document was updated or not
                if result.modified_count > 0:
                    updates_made += 1
        # If no updates were made, remove the meta and files documents
        if updates_made == 0:
            log_functions.deleteLog(db, str(process_id))
            print("No changes were made.")
        else:
            print(f'Field {update_field} updated successfully in all the documents. New value: {new_value}')
    else:
        print(f"The field {update_field} doesn't exist in any document in the collection.")

def updateFile(operation, db, collection_name, update_file, name, method):
    """
    Update multiple documents with information from a csv
    """
    if os.path.exists(update_file):
        # Import the update information
        update_data = pd.read_csv(update_file)

        # Get the fields:
        column_names = update_data.columns.to_list()
        field_to_match = column_names[0]  # The header from the first column will always be the field to match
        update_field = column_names[1]  # The header from the second column will always be the update field
        
        # Get the values from the columns
        values_to_match = update_data[field_to_match].values
        new_values = update_data[update_field].values

        print(f'There are {len(values_to_match)} objects to update:')

        # Access the collection:
        collection = db[collection_name]

        # Insert metadata about the update process
        process_id = log_functions.insertLog(db, name, method, operation, collection_name)

        # Prepare data for insertion into the files collection
        files_data = {
            "log_id": str(process_id),
            "operation": operation,
            field_to_match: values_to_match.tolist(),  # Convert numpy array to Python list
            update_field: new_values.tolist()  # Convert numpy array to Python list
        }

        # Access or create the files collection:
        files_collection = db["update_files"]

        # Insert the data into the files collection
        files_collection.insert_one(files_data)

        # For each row, use the update one function to modify the specific field stated in the file.
        updates_made = 0
        for value_to_match, new_value in zip(values_to_match, new_values):

            # Convert new_value to a list if it contains a semicolon, otherwise use it as is
            new_value_list = new_value.split(";") if ";" in new_value else new_value

            # Stable id of the object to be updated
            update_criteria = {field_to_match: value_to_match}

            # Find the document before the update to retrieve the previous value
            previous_document = collection.find_one(update_criteria)

            # Check if the document with the specified criteria exists in the collection
            if previous_document:
                # Check if the field exists in the document
                if update_field in previous_document:
                    # Get the previous value of the field
                    previous_value = previous_document.get(update_field)

                    # Update the log field in the JSON document
                    updated_log = log_functions.updateLog(previous_document, process_id, operation, update_field,
                                                         previous_value, new_value_list)

                    # Update the document with the new data
                    result = collection.update_one(update_criteria,
                                                   {"$set": {update_field: new_value_list, "log": updated_log}})
                    
                    # Print whether the document was updated or not
                    if result.modified_count > 0:
                        updates_made += 1
                        print(f'Field {update_field} updated successfully in the document with {list(update_criteria.keys())[0]}: {list(update_criteria.values())[0]}')
                        print('')
                    else:
                        print(f'Field {update_field} in the document with {list(update_criteria.keys())[0]}: {list(update_criteria.values())[0]} remains unchanged.')
                        print('')

                else:
                    print(f"The field {update_field} doesn't exist in the document.")

            else:
                print(f"The document you are searching for is not in the collection.")

        # If no updates were made, remove the meta and files documents
        if updates_made == 0:
            files_collection.delete_one({"meta_id": str(process_id)})
            log_functions.deleteLog(db, str(process_id))
            print("No changes were made.")
        else:
            print("Update done!")

    else:
        print(f'{update_file} file does not exist.')