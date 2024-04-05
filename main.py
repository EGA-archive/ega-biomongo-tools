#!/usr/bin/env python

"""main.py  :  Run any operation needed in the mongoDB """

__author__ = "Marta Huertas"
__version__ = "0.1"
__maintainer__ = "Marta Huertas"
__email__ = "marta.huertas@crg.eu"
__status__ = "development"

# Import packages
import sys
from pymongo import MongoClient
import conf
from source import mongoConnection, insertOne


# Functions
def print_help():
    print('USAGE:')
    print('This script combines different tools to manage the BioMongoDB in EGA')
    print('First thing to do is to write your needs in the conf.txt file')
    print('Follow the guidelines in that file.')

def connect_mongo():
    # Connect to MongoDB
    client = MongoClient(mongoConnection.mongo_host, mongoConnection.mongo_port, username=mongoConnection.username, password=mongoConnection.password, authSource=mongoConnection.auth_source)
    # Acces the database:
    db = client[conf.database_name]

    return db

def run_operation():
    """
    Run operations in the BioMongoDB by reading the conf file
    """
    # Get database connection:
    db = connect_mongo()

    if conf.operation == 'insert_one':
        insertOne.insert_one(db, conf.collection_name, conf.json_documents)