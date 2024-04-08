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
operation='insert_many' # Operations: insert_one, insert_many, update_one, update_many, update_all

# Metadata information
name="Marta Huertas"
method="EGAPRO"

# General information
database_name='mhuertas' # Name of the database (this is in test)
collection_name='study' # Collection to be managed (analysis, dac, dataset, experiment, policy, run, sample, study)

# Insert needs:
json_documents='./test_metadata/study/study_0.json' # Path to the json documents to be included in the MongoDB

# Update needs:
update_field='' # Target field to be updated
new_value=''

# Update_one needs:
ega_id='' # Id of the object to be modified

# Update many needs:
update_criteria={'field_to_match': 'value_to_match'}