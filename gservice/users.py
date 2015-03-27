#!/usr/bin/env python

# Filename: users.py

#=====================================================================#
# Copyright (c) 2015 Bradley Hilton <bradleyhilton@bradleyhilton.com> #
# Distributed under the terms of the GNU GENERAL PUBLIC LICENSE V3.   #
#=====================================================================#


import json
from datetime import datetime
from config import config

STAFF_DOMAIN = config.STAFF_DOMAIN
STUDENT_DOMAIN = config.STUDENT_DOMAIN

def date_obj_from_ISO8601(iso8601s):
	"""Returns datetime object from iso8601 string in Google JSON response"""
	return datetime.strptime(iso861s, '%Y-%m-%dT%H:%M:%S.000Z')

class UserType(object):
	def __init__(self, user_type=None):
		if user_type == 'Staff':
			# TODO (bixbydev): Get domain from config
			domain = STAFF_DOMAIN
		else:
			user_type = 'Student'
			domain = STUDENT_DOMAIN

		self.user_type = user_type
		self.domain = domain


class ExternalIds(UserType):
	def __init__(self, user_type):
		UserType.__init__(self, user_type)


class NewUser(UserType):
	"""docstring for NewUsers"""
	def __init__(self,
				 username, 
				 given_name, 
				 family_name, 
				 emails=[],
				 user_type=None,
				 external_userid=None, 
				 password=None,
				 suspended=False,
				 change_password=False,
				 org_unit='/Disabled Users'
				):
		# Proof of concept
		self.ut = UserType(user_type)
		self.user_type = self.ut.user_type
		# self.user_type = user_type
		self.username = username
		self.full_email_address = self.username + '@' + self.ut.domain
		self.given_name = given_name # First_Name
		self.family_name = family_name # Last_Name
		self.emails = emails.sort() # It really needs to be json or a dictionary
		self.external_userid = external_userid
		self.password = password
		self.suspended = suspended
		self.org_unit = org_unit

	def user_json(self):
		udict = {}
		udict['primaryEmail'] = self.full_email_address
		udict['name'] = {'givenName': self.given_name,
							'familyName': self.family_name}
		udict['suspended'] = self.suspended
		if self.password:
			udict['password'] = self.password
		udict['emails'] = {'address': self.full_email_address,
								'type': "work",
								'customType': "",
								'primary': True}
		udict['externalIds'] = {} # TODO (bixbydev): uid Structure
		udict['orgUnitPath'] = self.org_unit
		self.udict = udict


class UserFromJSON(object):
	def __init__(self, json_dict):
		assert(type(json_dict)) == dict
		self.json_dict = json_dict
		self.username = json_dict.get('primaryEmail')
		self.name = json_dict.get('name')
		if self.name:
			self.given_name = self.name.get('givenName')
			self.family_name = self.name.get('familyName')
		else:
			self.given_name = None
			self.family_name = None

		self.external_userid = json_dict.get('externalIds')
		if self.external_userid:
			for uid in self.external_userid:
				print uid # TODO (bixbydev): Eventually this will do soemthing

		self.password = json_dict.get('password')
		# Neat little hack. If the value isn't true it returns false
		self.change_password = (json_dict.get('changePasswordAtNextLogin') == True)
		self.suspended = (json_dict.get('suspended') == True)
		self.emails = json_dict.get('emails')
		for email in self.emails:
			print email
		self.email_aliases = self.json_dict.get('aliases')
		self.google_id = self.json_dict.get('id')


