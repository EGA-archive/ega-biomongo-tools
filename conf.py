#!/usr/bin/env python

"""conf.py  :  Configuration file """

####---- Configuration file to operate through BioMongo ----####

# ----------
# General information
# ----------
operation='insert_many' # Operations: insert_one, insert_many, update_one, update_all, update_with_file, restore_one, restore_all, add_empty_field, add_field_with_file, rename_field
name='Marta' # Name of the person that does this operation.
method='Testing insert many bulk' # Method used to obtain or modify the data (e.g. Raw data EGAPRO).
database_name='mhuertas' # Name of the database.
collection_name='experiment' # Collection to be managed (analysis, dac, dataset, experiment, file, policy, run, sample, study).


# Depending on the operation you should include the relevant information.

# ---------
# Insert needs:
# ----------
json_documents=f'./test_metadata/experiment/experiment_0.json' # Path to the json documents to be included in the MongoDB.

# ----------
# Update needs (update_field is always needed):
# ----------
update_field='' # Target field to be updated.
new_value='' # New value for the field (no need if using a file).
update_criteria={'field_to_match':'value_to_match'} # Criteria to update one, pick a field with unique values.
# If using update_with_file, please provide the csv with the information.
update_file = 'path/to/csv' # If you want to add a list as a new value, separate the values with ";"

# ----------
# Restore needs:
# ----------
restore_criteria={'field_to_match':'value_to_match'} # Criteria to restore one file.
log_id='' # Log id to the version to be restored.

# ---------
# Add new field needs:
# ----------
new_field='' # Name of the new field to be added. Please do not use empty spaces or special characters
# If using add_field_with_file, please provide the csv with the information. The structure should be the same as the update file.
new_field_file='path/to/csv'


# ----------
# Rename needs:
# ----------
field_name='' # Name of the field to be changed.
new_field_name='' # New name for the above stated field