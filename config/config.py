#!/usr/bin/python


# To-Do: 20150310
# I need to research password hashing
# http://stackoverflow.com/questions/2572099/pythons-safest-method-to-store-and-retrieve-passwords-from-a-database
# https://github.com/django/django/blob/master/django/contrib/auth/hashers.py#L216


import ConfigParser
import getpass


PASSWORD_SALT = None

# Set Scope GtestGoogleApps or GoogleApps
GSCOPE = 'GoogleApps'
# GSCOPE = 'GtestGoogleApps'

CONFIG_FILE = 'config/config.ini' # To-Do: Move this somewhere else

config = ConfigParser.ConfigParser(allow_no_value=True)
config.read(CONFIG_FILE)

# domain = config.get('GoogleApps', 'primarydomain')

PRIMARY_DOMAIN = config.get(GSCOPE, 'PRIMARY_DOMAIN')
STUDENT_DOMAIN = config.get(GSCOPE, 'STUDENT_DOMAIN')
CLIENT_ID = config.get(GSCOPE, 'CLIENT_ID')
CLIENT_SECRET = config.get(GSCOPE, 'CLIENT_SECRET')

MYSQL_PASSWORD = config.get('MySQL', 'password')


# if not MYSQL_PASSWORD:
# 	print 'No Password Set for MySQL'
# 	MYSQL_PASSWORD = getpass.getpass('Please enter the MySQL User Password: ')
# 	# MYSQL_PASSWORD = MYSQL_PASSWORD
# 
# print MYSQL_PASSWORD



def main():
	pass




if __name__ == '__main__':
	main()
