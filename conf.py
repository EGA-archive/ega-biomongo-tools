#!/usr/bin/env python

"""main.py  :  Run any operation needed in the mongoDB """

__author__ = "Marta Huertas"
__version__ = "0.1"
__maintainer__ = "Marta Huertas"
__email__ = "marta.huertas@crg.eu"
__status__ = "development"

####---- Configuration file to operate through BioMongo ----####

# First you should state the operation you need to perform.
# Depending on the operation you should include the relevant information.
operation='insert_one' # Operations: insert_one, insert_many, update_one, update_many

# General information
database_name='mhuertas' # Name of the database (this is in test)
collection_name='study' # Collection to be managed (analysis, dac, dataset, experiment, policy, run, sample, study)

# Insert needs:
json_documents='' # Path to the json documents to be included in the MongoDB

# Update needs:
update_field='' # Target field to be updated

# Update_one needs:
document_id='' # Id of the object to be modified
