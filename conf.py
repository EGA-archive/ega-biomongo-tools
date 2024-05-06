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
operation='' # Operations: insert_one, insert_many, update_one, update_many, update_all, update_with_file

# Metadata information
name='' # Name of the person that does this operation
method='' # Method used to obtain or modify the data (e.g. Raw data EGAPRO)

# General information
database_name='mhuertas' # Name of the database (this is in test)
collection_name='' # Collection to be managed (analysis, dac, dataset, experiment, policy, run, sample, study)

# Insert needs:
json_documents='path/to/json' # Path to the json documents to be included in the MongoDB

# Update needs:
update_field='' # Target field to be updated
new_value='' # New value for the field
update_criteria={'field_to_match':'value_to_match'} # Criteria to update one or many. If you want to update one, pick a field with unique values.
# If using update_with_file, please provide the csv with the information
update_file = './test_metadata/test.csv'