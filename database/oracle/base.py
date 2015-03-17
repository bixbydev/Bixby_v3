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
	import cx_Oracle
except ImportError, e:
	'The Module cx_Oracle is not installed'
	log.critical('Failed to load cx_Oracle Module: '+str(e))
	sys.exit(1)


class CursorWrapper(object):
	def __init__(self):
		try:
			self.connection = cx_Oracle.connect(config.ORA_USER+
												'/'+config.ORA_PASSWORD+
												'@'+config.ORA_HOST+
												'/'+config.ORA_SID)
			self.cursor = self.connection.cursor()
			log.info("Connected to Oracle Host: %s" %config.ORA_HOST)
		except cx_Oracle.DatabaseError, e:
			log.exception(e)


	def close(self):
		try:
			self.cursor
			self.cursor.close()
			self.connection.close()
			log.info('Oracle Connection Closed')
		except AttributeError:
			log.warn('No Oracle Cursor Open')


