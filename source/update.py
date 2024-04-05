#!/usr/bin/env python

"""update.py  :  Update specific or many documents in the desired MongoDB and collection """

__author__ = "Marta Huertas"
__version__ = "0.1"
__maintainer__ = "Marta Huertas"
__email__ = "marta.huertas@crg.eu"
__status__ = "development"

# Insert one function
def updateOne(db, collection_name, ega_id, update_field, new_value):
    """
    Insert one document in a specific database
    """
    # Access the collection:
    collection = db[collection_name]

        # Check if the document with the specified ID exists in the collection
    if collection.find_one({"stable_id": ega_id}):
        # Update the document
        result = collection.update_one({"stable_id": ega_id}, {"$set": {update_field: new_value}})
        
        # Print whether the document was updated or not
        if result.modified_count > 0:
            print(f'Field {update_field} updated successfully in the study with id {ega_id}')
        else:
            print(f"The field {update_field} already has the specific value")
    else:
        print(f"There's no document with id {ega_id} in the collection.")


        
