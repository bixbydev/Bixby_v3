#!/usr/bin/env python

# Filename: base.py

#=====================================================================#
# Copyright (c) 2015 Bradley Hilton <bradleyhilton@bradleyhilton.com> #
# Distributed under the terms of the GNU GENERAL PUBLIC LICENSE V3.   #
#=====================================================================#


import os
import sys
from config import config

from logger.log import log

log.debug('oracle.base Loaded')

try:
	import psycopg2
except ImportError, e:
	'The Module psycopg2 is not installed'
	log.critical('Failed to load Postgres psycopg2 Module: '+str(e))
	sys.exit(1)

connection_str = """dbname='{dbname}' 
					user='{user}' 
					password='{password}' 
					host='{host}' 
					port='{port}' 
					sslmode='require' """.format(dbname=config.PG_DB,
											user=config.PG_USER,
											password=config.PG_PASSWORD,
											host=config.PG_HOST,
											port=config.PG_PORT,
											)

class CursorWrapper(object):
	def __init__(self):
		try:

			self.connection = psycopg2.connect(connection_str)
			self.cursor = self.connection.cursor()
			log.info("Connected to Postgres Host: %s" %config.PG_HOST)
		except psycopg2.DatabaseError, e:
			log.exception(e)

	def close(self):
		self.cursor.close()
		log.info('Postgres Cursor Closed')
		self.connection.close()
		log.info('Postgres Connection Closed')