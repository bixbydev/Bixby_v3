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






class OrgUnit(object):
	def __init__(self, name, orgUnitPath, parentOrgUnitPath, description=None,
				blockInheritance=False, etag=None,  **kwargs):
		"""# TODO (bixbydev): Eventually Make the OU's managable by Bixby.
		OU's will need to move around under the parents if a parent ou is 
		cahnged. I'm abandoning this for now. OU's should be managed in the 
		admin console. Changes will be copied down from Google if the OU doesn't
		exist. This could be dangerous if the OU changes and is linked to a PK.


		"""
		self.name = name
		self.orgUnitPath = orgUnitPath
		self.parentOrgUnitPath = parentOrgUnitPath
		self.description = description
		# self.blockInheritance = blockInheritance

		self.etag = etag
		if kwargs['orgUnitId']:
			self.google_ouid = kwargs['orgUnitId']

		if kwargs['parentOrgUnitId']:
			self.google_parent_ouid = kwargs['parentOrgUnitId']

		self.initial_request = self._return_request()

	def _return_request(self):
		request_dict = { 'name': self.name,
						'orgUnitPath': self.orgUnitPath,
						'parentOrgUnitPath': self.parentOrgUnitPath,
						'description': self.description,
						'blockInheritance': self.blockInheritance,
						'kind': 'admin#directory#orgUnit',
						}

		return request_dict

	def new_request(self):
		if self.name != self.initial_request['name']:
			self.orgUnitPath
		return self._return_request()



		

class OrgUnitFromJSON(OrgUnit):
	def __init__(self, data):
		assert(type(data)) == dict
		OrgUnit.__init__(self, **data)



class BixbyOrgUnit(database.mysql.base.CursorWrapper, OrgUnitFromJSON, OrgUnit):
	def __init__(self):
		database.mysql.base.CursorWrapper.__init__(self)
		# self.null = None

	def read_ou_from_json(self, json_object):
		self.json_ou = OrgUnitFromJSON(json_object)

	def read_ou_from_db(self, ou_path):
		pass

	def _compare_ou(self, ou_name, ou_fullpath, ou_parent_path, ou_description, 
					ou_etag=None, **kwargs):
		if self.name != ou_name:
			print ou_name

		if self.orgUnitPath != ou_path:
			print ou_path

	def _ou_exists(self, ou_fullpath):
		self.cursor.execute("""SELECT * FROM org_unit WHERE ORG_UNIT_PATH = '%s'""" %(ou_fullpath))
		self.raw_columns = self.cursor.fetchone()
		if self.raw_columns == None:
			return False

		else:
			return True

	def commit_changes(self):
		pass


class GoogleOUs(object):
	def __init__(self, directory_service):
		self.ou_service = directory_service.orgunits()
		self.customerId = 'my_customer'

	
	def _get_ou(self, ou_path):
		return ou_service.get(customerId=self.customerId
											, orgUnitPath=ou_path)

	def _get_all_ous(self):
		all_ous_json = self.ou_service.list(customerId=self.customerId
											, type='all', orgUnitPath=None)
		return all_ous_json['organizationUnits']



class Date(object):

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
	
