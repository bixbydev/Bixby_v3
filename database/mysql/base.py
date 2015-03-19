#!/usr/bin/env python

# Filename: base.py

#=====================================================================#
# Copyright (c) 2015 Bradley Hilton <bradleyhilton@bradleyhilton.com> #
# Distributed under the terms of the GNU GENERAL PUBLIC LICENSE V3.   #
#=====================================================================#


import os
import sys
import time
from config import config

from logger.log import log

log.debug('mysql.base Loaded')
print config.MYSQL_BACKUPPATH

try:
	import MySQLdb
except ImportError, e:
	'The Module MySQLdb is not installed'
	log.critical('Failed to load MySQLdb Module: '+str(e))
	sys.exit(1)


def backup_mysql():
	"""Backups the DB until things get very large I am going to do this every 
		time. Or until I am sure my code is good."""
	dnsdt = str(time.strftime('%Y%m%d%H%M%S', time.localtime()))
	log.info("""Creating mysqldump: PS_PY_Dev_Back.'%s'.sql""" %dnsdt)
	os.system("""mysqldump -h '%s' -u '%s' -p'%s' '%s' > '%s'.'%s'.sql""" \
														%(config.MYSQL_HOST,
														  config.MYSQL_USER, 
														  config.MYSQL_PASSWORD,
														  config.MYSQL_DB, 
														  dnsdt))

def restore_mysql(db, sqlfile):
	if not os.path.exists(sqlfile):
		raise TypeError("""This is totally the wrong error because 
							I don't know the right error""")

	log.info("Restoring DB: %s from File: %s" %(db, sqlfile))	
	os.system("""mysql -h '%s' -u '%s' -p'%s' '%s' < %s""" \
													%(config.MYSQL_HOST,
													  config.MYSQL_USER, 
													  config.MYSQL_PASSWORD,
															  db, sqlfile))


class CursorWrapper(object):
	"""Wrapper to open a MySQL Connection and creates a Cursor"""
	def __init__(self, host=config.MYSQL_HOST,
                       user=config.MYSQL_USER,
                       passwd=config.MYSQL_PASSWORD,
                       db=config.MYSQL_DB):

		self.example = 'Testing'
		self.connection = MySQLdb.connect (host = host,
                       						user = user,
                       						passwd = passwd,
                       						db = db)
		self.cursor = self.connection.cursor()
		log.info("""Setting autocommit = \"True\"""")
		self.connection.autocommit(True)
		log.info("Connected to MySQL Host: %s Database: %s" % (host, db))

	def close(self):
		self.cursor.close()
		log.info('MySQL Cursor Closed')
		# self.connection.commit()
		self.connection.close()
		log.info('MySQL Connection Closed')