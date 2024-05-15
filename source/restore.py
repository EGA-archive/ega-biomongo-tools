#!/usr/bin/env python

"""restore.py  :  Reset to previous version as saved in meta """

__author__ = "Marta Huertas"
__version__ = "0.1"
__maintainer__ = "Marta Huertas"
__email__ = "marta.huertas@crg.eu"
__status__ = "development"

from . import logScript

def restoreOne(operation, db, collection_name, reset_criteria, meta_id, name, method):
    """
    Reset field in a document to a previous version using meta_info.
    """
    # Access the collection:
    collection = db[collection_name]

    # Find the document
    document = collection.find_one(reset_criteria)

    if document: # If the specific document we want to restore exists:
        # Insert metadata about the reset process in the meta collection
        process_id = logScript.insertLog(db, name, method, operation, collection_name)

        # Retrieve the 'meta_info' field
        meta_info = document.get('meta_info', [])

        # Find the index of the desired version in the 'meta_info' array
        meta_test = False # Test if the meta_id exists
        for update in meta_info:
            if update.get('meta_id') == meta_id:
                meta_test = True
                if 'update' in update.get('operation'):
                    # Get the field and value to restore
                    update_field = update.get('modified_field')
                    new_value = update.get('previous_value')

                    if meta_test:
                        # Delete meta_info instance for the specific meta_id
                        updated_meta_info = [update for update in meta_info if update.get('meta_id') != meta_id]

                        # Define the new metadata to be added
                        new_meta_info = {
                            "meta_id" : str(process_id),
                            "operation": operation,
                            "previous_meta_id": meta_id
                            }

                        # Merge the new metadata with the existing meta_info
                        updated_meta_info.insert(0, new_meta_info)

                        # Update the document with the new data
                        result = collection.update_one(reset_criteria, {"$set": {update_field: new_value, "meta_info": updated_meta_info}})

                        print(f'Document restored to version with id: {meta_id}.')
                    else:
                        print('Meta id not found in this document. No change made.')
                else: 
                    print('Insert operations cannot be restored.')
    else:
        print('The document you are searching for is not in the collection.')