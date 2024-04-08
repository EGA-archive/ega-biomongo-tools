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
    Update one document in a specific collection from database
    """
    # Access the collection:
    collection = db[collection_name]

        # Check if the document with the specified ID exists in the collection
    if collection.find_one({"stable_id": ega_id}):
        # Update the document
        result = collection.update_one({"stable_id": ega_id}, {"$set": {update_field: new_value}})
        
        # Print whether the document was updated or not
        if result.modified_count > 0:
            print(f'Field {update_field} updated successfully in the document with id {ega_id}')
        else:
            print(f"The field {update_field} already has the specific value")
    else:
        print(f"There's no document with id {ega_id} in the collection.")

def updateAll(db, collection_name, update_field, new_value):
    """
    Update all documents in a specific collection from database
    """
    # Access the collection:
    collection = db[collection_name]

    # Update all documents in the collection
    result = collection.update_many({}, {"$set": {update_field: new_value}})
        
    print(f'Field {update_field} updated successfully in all the documents with {new_value}')
        
        
# Update documents function
def updateMany(db, collection_name, update_criteria, update_field, new_value):
    """
    Update multiple documents in a specific collection based on given criteria
    """
    # Access the collection
    collection = db[collection_name]
    
    # Update multiple documents in the collection
    result = collection.update_many(update_criteria, {"$set": {update_field: new_value}})
    
    # Print the number of documents updated
    print(f'Field {update_field} updated successfully with {new_value} in {result.modified_count} documents')