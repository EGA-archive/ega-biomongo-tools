#!/usr/bin/env python

"""mongoConnection.py  :  Credentials for MongoDB connection """

__author__ = "Marta Huertas"
__version__ = "0.1"
__maintainer__ = "Marta Huertas"
__email__ = "marta.huertas@crg.eu"
__status__ = "development"

# Connection details:
mongo_host = 'localhost'  # Since you're using SSH tunneling
mongo_port = 27018  # The local port you've forwarded to the remote MongoDB instance
username = 'root'  # MongoDB root username
password = 'example'  # MongoDB root password
auth_source = 'admin'  # The authentication database