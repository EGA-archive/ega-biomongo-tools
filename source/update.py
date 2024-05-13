#!/usr/bin/env python

"""update.py  :  Update specific or many documents in the desired MongoDB and collection """

__author__ = "Marta Huertas"
__version__ = "0.1"
__maintainer__ = "Marta Huertas"
__email__ = "marta.huertas@crg.eu"
__status__ = "development"

from . import meta
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
            # Get the previous value of the field
            previous_value = previous_document.get(update_field)

            # Insert metadata about the update process in the meta collection
            process_id = meta.insertMeta(db, name, method, operation, collection_name)

            # Update the meta_info field in the JSON document
            updated_meta_info = meta.updateMeta(previous_document, process_id, operation, update_field, previous_value)

            # Update the document with the new data
            result = collection.update_one(update_criteria, {"$set": {update_field: new_value, "meta_info": updated_meta_info}})
            
            # Print whether the document was updated or not
            if result.modified_count > 0:
                print(f'Field {update_field} updated successfully in the document with {list(update_criteria.keys())[0]}: {list(update_criteria.values())[0]}')
                print('')
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
    
    # Check if the field exists in at least one document in the collection
    if collection.find_one({update_field: {"$exists": True}}):
        # Insert metadata about the update process
        process_id = meta.insertMeta(db, name, method, operation, collection_name)
        
        # Iterate over each document to retrieve previous values and update with metadata
        for previous_document in previous_documents:
            # Check if the field exists in the document
            if update_field in previous_document:
                # Get the previous value of the field
                previous_value = previous_document.get(update_field)

                # Update the meta_info field in the JSON document
                updated_meta_info = meta.updateMeta(previous_document, process_id, operation, update_field, previous_value)

                # Update the document with the new metadata
                collection.update_one({"_id": previous_document["_id"]}, {"$set": {update_field: new_value, "meta_info": updated_meta_info}})
        
        print(f'Field {update_field} updated successfully in all the documents. New value: {new_value}')
    else:
        print(f"The field {update_field} doesn't exist in any document in the collection.")

def updateFile(operation, db, collection_name, update_file, update_field, name, method):
    """
    Update multiple documents with information from a csv
    """
    if os.path.exists(update_file):
        # Import the update information
        update_data = pd.read_csv(update_file)
        stable_ids = update_data["stable_id"].values
        new_values = update_data["new_value"].values

        print(f'There are {len(stable_ids)} objects to update:')

        # Access the collection:
        collection = db[collection_name]
        
        # Insert metadata about the update process
        process_id = meta.insertMeta(db, name, method, operation, collection_name)
        
        # For each row, use the update one functio to modify the specific field stated in the file.
        for stable_id, new_value in zip(stable_ids, new_values):
            # Stable id of the object to be updated
            update_criteria = {'stable_id' : stable_id}

            # Find the document before the update to retrieve the previous value
            previous_document = collection.find_one(update_criteria)
            
            # Check if the document with the specified criteria exists in the collection
            if previous_document:
                # Check if the field exists in the document
                if update_field in previous_document:
                    # Get the previous value of the field
                    previous_value = previous_document.get(update_field)

                    # Update the meta_info field in the JSON document
                    updated_meta_info = meta.updateMeta(previous_document, process_id, operation, update_field, previous_value)

                    # Update the document with the new data
                    result = collection.update_one(update_criteria, {"$set": {update_field: new_value, "meta_info": updated_meta_info}})
                    
                    # Print whether the document was updated or not
                    if result.modified_count > 0:
                        print(f'Field {update_field} updated successfully in the document with {list(update_criteria.keys())[0]}: {list(update_criteria.values())[0]}')
                        print('')
                else:
                    print(f"The field {update_field} doesn't exist in the document.")
            else:
                print(f"The document you are searching for is not in the collection.") 
        print("Update done!")
    else:
        print(f'{update_file} file does not exist')