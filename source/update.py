#!/usr/bin/env python

"""update.py  :  Update specific or many documents in the desired MongoDB and collection """

__author__ = "Marta Huertas"
__version__ = "0.1"
__maintainer__ = "Marta Huertas"
__email__ = "marta.huertas@crg.eu"
__status__ = "development"

# Insert one function
def updateOne(db, collection_name, update_criteria, update_field, new_value):
    """
    Update one document in a specific collection from database
    """
    # Access the collection:
    collection = db[collection_name]
    
    # Check if the document with the specified criteria exists in the collection
    if collection.find_one(update_criteria):
        # Update the document
        result = collection.update_one(update_criteria, {"$set": {update_field: new_value}})
        
        # Print whether the document was updated or not
        if result.modified_count > 0:
            print(f'Field {update_field} updated successfully in the document with {list(update_criteria.keys())[0]}: {list(update_criteria.values())[0]}')
        else:
            print(f"The field {update_field} already has the specific value")
    else:
        print(f"The document you are searching for is not in the collection.")


def updateAll(db, collection_name, update_field, new_value):
    """
    Update all documents in a specific collection from database
    """
    # Access the collection:
    collection = db[collection_name]

    # Update all documents in the collection
    result = collection.update_many({}, {"$set": {update_field: new_value}})
        
    print(f'Field {update_field} updated successfully in all the documents. New value: {new_value}')
        
        
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
    print(f'Field {update_field} updated successfully in {result.modified_count} documents. New value: {new_value}')