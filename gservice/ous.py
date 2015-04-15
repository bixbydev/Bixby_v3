#!/usr/bin/env python

# Filename: ous.py

#=====================================================================#
# Copyright (c) 2015 Bradley Hilton <bradleyhilton@bradleyhilton.com> #
# Distributed under the terms of the GNU GENERAL PUBLIC LICENSE V3.   #
#=====================================================================#

""" This module is temporarily abandoned due to preassure to finish other 
core features """


import json
import csv

# import services
from config.config import OU_CSV_FILE_PATH
from database.mysql.base import CursorWrapper

# ou_service = services.directory_service.orgunits()

# db = database.mysql.base.CursorWrapper()
# dbc = db.cursor


class BaseOU(dict):
	def __init__(self, name, orgUnitPath, parentOrgUnitPath, description=None,
		blockInheritance=False, kind='admin#directory#orgUnit#', **kwargs):
		dict.__init__(self)
		self['name'] = name
		self['orgUnitPath'] = orgUnitPath
		self['parentOrgUnitPath'] = parentOrgUnitPath
		self['description'] = description
		self['blockInheritance'] = blockInheritance
		self['kind'] = kind

	def __setitem__(self, key, value):
		dict.__setitem__(self, unicode(key), unicode(value))
		self.__setattr__(key, unicode(value))
		print key, value


class JSONRequestOU(BaseOU):
	def __init__(self, json_object):
		assert(type(json_object)) == dict
		BaseOU.__init__(self, **json_object)
		
		if json_object.get('orgUnitId'):
			self['orgUnitId'] = json_object['orgUnitId']

		if json_object.get('parentOrgUnitId'):
			self['parentOrgUnitId'] = json_object['parentOrgUnitId']


	@classmethod
	def return_json_request(cls, json_object):
		data = cls(json_object)
		return data


class DatabaseOU(CursorWrapper):
	def __init__(self):
		CursorWrapper.__init__(self)

	def _get_ou_full_path(self, department_id, map_id, user_type=None):
		params = (department_id, map_id)
		q = """SELECT OU_PATH 
					FROM orgunit 
					WHERE DEPARTMENT_ID = %s
						AND MAP_KEY = %s"""
		if user_type:
			params += (user_type,)
			q += """AND USER_TYPE = %s"""

		self.cursor.execute(q, params)
		return '/'+self.cursor.fetchone()[0]

	@classmethod
	def get_ou_path(cls, department_id, map_id, user_type=None):
		data = cls()
		return data._get_ou_full_path(department_id, map_id, user_type=None)
		
		


class BixbyOU(JSONRequestOU, CursorWrapper):
	db_fields_map = {'orgUnitPath': 'OU_PATH',
					'name': 'OU_NAME',
					'parentOrgUnitPath': 'PARENT_OU_PATH',
					'description': 'DESCRIPTION',
					'blockInheritance': 'BLOCK_INHERITANCE',
					'orgUnitId': 'GOOGLE_OUID',
					'parentOrgUnitId': 'GOOGLE_PARENT_OUID',
					'etag': 'ETAG'}
	def __init__(self):
		self.CursorWrapper.__init__(self)

	def _get_db_orgunitid(self, ou):
		self.ou = ou
		self.cursor.execute("""SELECT ID FROM ORGUNIT WHERE OU_PATH = %s""", (ou))
		return self.cursor.fetchone()


	def _json_object_from_db(self, ou):

		self.cursor.execute("""SELECT OU_PATH
								, OU_NAME
								, PARENT_OU_PATH
								, DESCRIPTION
								, BLOCK_INHERITANCE
								, GOOGLE_OUID
								, GOOGLE_PARENT_OUID
								, ETAG 
								FROM ORGUNIT 
								WHERE OU_PATH = %s""", (ou))
		keys = self.cursor.description
		result = self.cursor.fetchone()
		d = {}
		for k, v in zip(keys, result):
			d[k] = v

		self.return_json_request(d)


	def _ou_exists_in_db(self, ou):
		"""Check if the OU exists in the DB"""
		self.cursor.execute("""SELECT ORG_UNIT_PATH FROM ORGUNIT WHERE OU_PATH = %s""", (ou,))
		ou_in_db = self.cursor.fetchone()

		if ou_in_db:
			return True
		else:
			return False




	def insert_into_db(self, orgUnitPath, name, parentOrgUnitPath, description, block):
		pass

