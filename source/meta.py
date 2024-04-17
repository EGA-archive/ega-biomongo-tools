#!/usr/bin/env python

""" meta.py  :  Generate meta information about the process """

__author__ = "Marta Huertas"
__version__ = "0.1"
__maintainer__ = "Marta Huertas"
__email__ = "marta.huertas@crg.eu"
__status__ = "development"


# Import Packages
from datetime import datetime

def insertMeta(db, name, method, operation, collection_name):
    """
    Function to generate an entrance to the meta collection with information about the process.
    """
    # Insert metadata into the 'meta' collection
    meta_collection = db['meta']
    process_info = {
        # The id is created by MongoDB, no need to generate one.
        "name": name,
        "operation" : operation,
        "collection": collection_name,
        "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "method": method
    }
    meta_result = meta_collection.insert_one(process_info)

    return meta_result.inserted_id
