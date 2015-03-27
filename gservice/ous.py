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



class OrgUnit(object):
	def __init__(self, name, orgUnitPath, parentOrgUnitPath, description,
				etag=None, **kwargs):
		self.name = name
		self.orgUnitPath = orgUnitPath
		self.parentOrgUnitPath = parentOrgUnitPath
		self.description = description
		self.etag = etag
		self.init_dict = self._create_request_obj()

	def _return_request_obj(self):
		request_dict = { 'name': self.name,
						'orgUnitPath': self.orgUnitPath,
						'parentOrgUnitPath': self.parentOrgUnitPath,
						'description': self.description }
		if self.etag:
			request_dict['etag'] = self.etag

		return request_dict

	def print_stuff(self):
		print self.name, self.parentOrgUnitPath
		

class OrgUnitFromJSON(OrgUnit):
	def __init__(self, dictionary):
		assert(type(dictionary)) == dict
		OrgUnit.__init__(self, **dictionary)


class BixbyOrgUnit(database.mysql.base.CursorWrapper, OrgUnitFromJSON, OrgUnit):
	def __init__(self):
		database.mysql.base.CursorWrapper.__init__(self)
		self.null = None

	def read_ou_from_json(self, json_object):
		self.google_json = OrgUnitFromJSON(json_object)

	def read_ou_from_db(self, ou_path):
		pass

	def _compare_ou(self, ou_name, ou_path, ou_parent_path, ou_description, 
					ou_etag=None, **kwargs):
		if self.name != ou_name:
			print ou_name

		if self.orgUnitPath != ou_path:
			print ou_path


