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

GTIME_FORMAT = '%Y-%m-%dT%H:%M:%S.000Z'
GEXPIRY_FORMAT = '%Y-%m-%dT%H:%M:%SZ'


user_map = {'bixbyId': 'ID',
			 'suspended': 'SUSPENDED',
			 'primaryEmail': 'PRIMARY_EMAIL',
			 'creationTime': 'CREATION_DATE',
			 'id': 'GOOGLE_ID',
			 'includeInGlobalAddressList': 'GLOBAL_ADDRESSBOOK',
			 'lastLoginTime': 'LASTLOGIN_DATE',
			 'familyName': 'LAST_NAME',
			 'givenName': 'FIRST_NAME',
			 'orgUnitPath': 'OU_PATH',
			 'changePasswordAtNextLogin': 'CHANGE_PASSWORD',
			 'externalIds': 'EXTERNAL_UID',
			 'etag': 'ETAG'}


def return_datetime(iso8601s):
	"""Returns datetime object from iso8601 string in Google JSON response"""
	try:
		return datetime.strptime(iso861s, GTIME_FORMAT)
	except ValueError:
		"""Handles the Google Oauth Expiry Format"""
		return datetime.strptime(iso861s, GEXPIRY_FORMAT)





class UserType(object):
	USER_TYPE_MAP = {u'staff': 
						{u'userTypeId': 1,
							u'domain': STAFF_DOMAIN,
							u'defaultOU': u'Staff'},
						u'student': {u'userTypeId': 2,
							u'domain': STUDENT_DOMAIN,
							u'defaultOU': u'Schools'} 
						}
	user_type_map = config.USER_TYPE_MAP
	# user_type_map = config.USER_TYPE_MAP
	def __init__(self, user_type=None):
		self.user_type = user_type
		self.domain = self.user_type_map[user_type]['domain']
		self.user_type_id = self.user_type_map[user_type]['userTypeId']
		self.default_org_unit = self.user_type_map[user_type]['defaultOU']





class BaseUser(UserType):
	*"""docstring for BaseUser"""
	def __init__(self,
				 primary_email, 
				 given_name, 
				 family_name,
				 user_type=None,
				 external_uid=None, 
				 password=None,
				 suspended=False,
				 change_password=False,
				 org_unit='/Schools'
				):
		UserType.__init__(user_type)
		self.primary_email = primary_email
		self.full_email_address = self.username + '@' + self.ut.domain
		self.given_name = given_name # First_Name
		self.family_name = family_name # Last_Name
		self.external_userid = external_userid
		self.password = password
		self.suspended = suspended
		self.org_unit = org_unit


	def user_object(self):
		pass








class UserFromJSON(object):
	def __init__(self, data):
		assert(type(data)) == dict
		self.data = data
		self.username = data.get('primaryEmail')
		self.name = data.get('name')
		if self.name:
			self.given_name = self.name.get('givenName')
			self.family_name = self.name.get('familyName')
		else:
			self.given_name = None
			self.family_name = None

		self.external_uid = data.get('externalIds')
		if self.external_uid:
			for uid in self.external_uid:
				print uid.get('customType'), uid.get('value')  # TODO (bixbydev): Eventually this will do soemthing

		self.password = data.get('password')
		
		self.change_password = (data.get('changePasswordAtNextLogin') == True)
		self.suspended = (data.get('suspended') == True)
		self.emails = data.get('emails')
		for email in self.emails:
			print email
		self.email_aliases = self.data.get('aliases')
		self.google_id = self.data.get('id')









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


class OldUserFromJSON(object):
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


