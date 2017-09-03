#!/usr/bin/env python

# Filename: services.py

#=====================================================================#
# Copyright (c) 2017 Bradley Hilton <bradleyhilton@bradleyhilton.com> #
# Distributed under the terms of the GNU GENERAL PUBLIC LICENSE V3.   #
#=====================================================================#

import csv
import json
import time

from gservice.directoryservice import DirectoryService
from logger.log import log

external_ids = """{"externalIds": [{"customType": "%s", "type": "custom", "value": "%s"}]}"""

# csv_file = """/home/bhilton/UPDATE Student ISI_EXTERNAL_ID.csv"""
csv_file = """/home/bhilton/UPDATE ISI_EXTERNAL_ID Test File.csv"""

with open(csv_file, 'rub') as csvfile:
	ds = DirectoryService()
	us = ds.users()
	csvreader = csv.reader(csvfile, delimiter=',')
	for row in csvreader:
		print 'UPDATING: '+str(row)
		update = json.loads(external_ids % (row[2], row[3]))
		print update
		try:
			result = us.patch(userKey=row[1], body=update).execute()
		except:
			print Exception
			exit(0)

		print result
		print "-----------"
		time.sleep(.2)


