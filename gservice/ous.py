#!/usr/bin/env python

# Filename: ous.py

#=====================================================================#
# Copyright (c) 2015 Bradley Hilton <bradleyhilton@bradleyhilton.com> #
# Distributed under the terms of the GNU GENERAL PUBLIC LICENSE V3.   #
#=====================================================================#

import json
# import services
import database.mysql.base

# ou_service = services.directory_service.orgunits()

# db = database.mysql.base.CursorWrapper()
# dbc = db.cursor


def pull_sync_org_unit():
	pass


db_fields_map = {'name': 'ORG_UNIT_NAME',
				'orgUnitPath': 'ORG_UNIT_PATH',
				'parentOrgUnitPath': 'PARENT_OU_PATH',
				'description': 'DESCRIPTION',
				'blockInheritance': 'BLOCK_INHERITANCE',
				'orgUnitId': 'GOOGLE_OUID',
				'parentOrgUnitId': 'GOOGLE_PARENT_OUID'}



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
		dict.__setitem__(self, key, value)
		self.__setattr__(key, value)
		print key, value


class JsonOU(BaseOU):
	def __init__(self, json_object):
		assert(type(json_object)) == dict
		BaseOU.__init__(self, **json_object)
		
		if json_object.get('orgUnitId'):
			self['orgUnitId'] = json_object['orgUnitId']

		if json_object.get('parentOrgUnitId'):
			self['parentOrgUnitId'] = json_object['parentOrgUnitId']





class Date(object):
	"""Learning how to use classmethod
	TODO (bixbydev): Remove This Class"""
	day = 0
	month = 0
	year = 0
	dow = 1

	def __init__(self, day=day, month=0, year=0):
		self.day = day
		self.month = month
		self.year = year
		self._x = None

	@classmethod
	def from_string(cls, date_as_string):
		day, month, year = map(int, date_as_string.split('-'))
		date1 = cls(day, month, year)
		return date1

	@property
	def dom(self):
		return self.day
	
