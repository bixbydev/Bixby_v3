#!/usr/bin/env python

# Filename: groups.py

#=====================================================================#
# Copyright (c) 2015 Bradley Hilton <bradleyhilton@bradleyhilton.com> #
# Distributed under the terms of the GNU GENERAL PUBLIC LICENSE V3.   #
#=====================================================================#

import json
import time

from googleapiclient.model import makepatch
from config import config
from database.mysql.base import CursorWrapper
from database import queries
from logger.log import log
from gservice.directoryservice import DirectoryService
from util import un_map, return_datetime, json_date_serial


bixby_group = {
        "nonEditableAliases": [
            "cb@berkeley.k12.ca.us"
        ], 
        "kind": "admin#directory#group", 
        "name": "Chromebook Group", 
        "adminCreated": True, 
        "directMembersCount": "2", 
        "email": "cb@berkeley.net", 
        "etag": "MO4FtId2-yiZq_-3TpU3AZTf2Ak/0kWGJwh6V6Qq7TsWsQ5WOBjHmVc", 
        "id": "00z337ya1mm5j0v", 
        "description": "Forwards the Google Chromebook emails. Contact Jay if you want to post/join the main group."
    }

valid_types = {'string': str,
				'boolean': bool,
				'integer':}

group_schema = """{
    "title": "Bixby Group Schema",
    "type": "object",
	"schemaVersion": "v1",
	"dbTable": "group",
    "properties": {
        "name": {
            "type": "string",
            "required": true,
            "column": "GROUP_NAME"
        },
        "adminCreated": {
        	"description": "Group was created by an Admin User. Read-Only",
            "type": "boolean",
            "readonly": true
        },
        "directMembersCount": {
            "description": "Group direct members count",
            "type": "integer",
            "readonly": true
        },
        "email": {
            "type": "string",
            "required": true,
            "column": "GROUP_EMAIL"
        },
        "id": {
            "type": "string",
            "required": true,
            "column": "GOOGLE_ID"
        },
        "description": {
            "type": "string",
            "column": "DESCRIPTION"
            }
    }
}"""

class ValidType(object):
	def __init__(self, data_type, value):
		self.data_type = data_type
		self.value = value





class BaseGroup(object):
	schema = json.loads(group_schema)
	schema_props = schema.get('properties')
	def __init__(self,
				name=None,
				adminCreated=True,
				directMambersCount=0,
				email=None,
				etag=None,
				id=None,
				description=None,
				kind="admin#directory#group"
				):

		self.api_payload = {}
		self.db_payload = {}

		if name:
			self.name = name

	def __setattr__(self, name, value):
		object.__setattr__(self, name, value)
		if name in self.schema_props.iterkeys():
			self.api_payload[name] = value
			db_col = self.schema_props.get(name).get('column', None)
			if db_col:
				self.db_payload[db_col] = value
