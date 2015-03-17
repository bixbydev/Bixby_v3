#!/usr/bin/env python

# Filename: config.py

#=====================================================================#
# Copyright (c) 2015 Bradley Hilton <bradleyhilton@bradleyhilton.com> #
# Distributed under the terms of the GNU GENERAL PUBLIC LICENSE V3.   #
#=====================================================================#


# To-Do: 20150310
# I need to research password hashing
# http://stackoverflow.com/questions/2572099/pythons-safest-method-to-store-and-retrieve-passwords-from-a-database
# https://github.com/django/django/blob/master/django/contrib/auth/hashers.py#L216

import os
from ConfigParser import ConfigParser
import getpass

from logger.log import log

log.debug('The Config File is Logging')

CONFIG_FILE = 'config/config.ini' # To-Do: Move this somewhere else
config = ConfigParser(allow_no_value=True)
config.read(CONFIG_FILE)

PASSWORD_SALT = None

# Google Service Configuration Section
# Set Scope GtestGoogleApps or GoogleApps
# GSCOPE = 'GoogleApps'
GSCOPE = 'GtestGoogleApps'
PRIMARY_DOMAIN = config.get(GSCOPE, 'PRIMARY_DOMAIN')
STUDENT_DOMAIN = config.get(GSCOPE, 'STUDENT_DOMAIN')
CLIENT_ID = config.get(GSCOPE, 'CLIENT_ID')
CLIENT_SECRET = config.get(GSCOPE, 'CLIENT_SECRET')


# MySQL Configuration Section
MYSQL_BACKUPPATH = config.get('MySQL', 'backup_path')
MYSQL_DB = config.get('MySQL', 'db')
MYSQL_HOST = config.get('MySQL', 'host')
MYSQL_USER = config.get('MySQL', 'user')

MYSQL_PASSWORD = config.get('MySQL', 'password')
if not MYSQL_PASSWORD:
	print 'No Password Set for MySQL'
	MYSQL_PASSWORD = getpass.getpass('Please enter the MySQL User Password: ')

# If the MYSQL_BACKUPATH does not exist (DNE); create it.
if not os.path.exists(MYSQL_BACKUPPATH) and MYSQL_BACKUPPATH:
	log.info('Creating the MYSQL_BACKUP directory at: %s' %MYSQL_BACKUPPATH)
	os.makedirs(MYSQL_BACKUPPATH)


# Oracle Configuration Section
ORA_HOST = config.get('Oracle', 'host')
ORA_SID = config.get('Oracle', 'sid')
ORA_USER = config.get('Oracle', 'user')

ORA_PASSWORD = config.get('Oracle', 'password')
if not ORA_PASSWORD:
	print 'No Password Set for Oracle'
	ORA_PASSWORD = getpass.getpass('Please enter the Oracle User Password: ')

ORA_CONNECTION_STRING = None


def main():
	pass


if __name__ == '__main__':
	main()
