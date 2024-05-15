#!/usr/bin/env python

"""conf.py  :  Configuration file """

####---- Configuration file to operate through BioMongo ----####

# ----------
# General information
# ----------
operation='' # Operations: insert_one, insert_many, update_one, update_all, update_with_file, restore_one
name='' # Name of the person that does this operation.
method='' # Method used to obtain or modify the data (e.g. Raw data EGAPRO).
database_name='' # Name of the database.
collection_name='' # Collection to be managed (analysis, dac, dataset, experiment, policy, run, sample, study).


# Depending on the operation you should include the relevant information.

# ---------
# Insert needs:
# ----------
json_documents=f'path/to/json' # Path to the json documents to be included in the MongoDB.

# ----------
# Update needs (update_field is always needed):
# ----------
update_field='' # Target field to be updated.
new_value='' # New value for the field (no need if using a file).
update_criteria={'field_to_match':'value_to_match'} # Criteria to update one, pick a field with unique values.
# If using update_with_file, please provide the csv with the information.
update_file = 'path/to/csv'

# ----------
# Restore needs:
# ----------
restore_criteria={'field_to_match':'value_to_match'} # Criteria to restore one file.
log_id='' # Meta id to the version to be restored.