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
	USER_TYPE_DEFAULTS = {u'staff': 
						{u'userTypeId': 1,
							u'domain': STAFF_DOMAIN,
							u'defaultOU': u'Staff',
							u'change_password': True,
							u'external_uid_name': 'staff'},
						u'student': {u'userTypeId': 2,
							u'domain': STUDENT_DOMAIN,
							u'defaultOU': u'Schools',
							u'change_password': False,
							u'external_uid_name': 'student'}
						}
	user_type_map = USER_TYPE_DEFAULTS
	# user_type_map = config.USER_TYPE_MAP
	def __init__(self, user_type=None):
		self.user_type = user_type
		self.domain = self.user_type_map[user_type]['domain']
		self.user_type_id = self.user_type_map[user_type]['userTypeId']
		self.default_org_unit = self.user_type_map[user_type]['defaultOU']
		self.change_password = self.user_type_map[user_type]['change_password']
		self.external_uid_type = self.user_type_map[user_type]['external_uid_name']


class BaseUser(UserType):
	"""docstring for BaseUser"""
	def __init__(self,
				 user_type=None,
				 primary_email=None, 
				 given_name=None, 
				 family_name=None,
				 external_uid=None, 
				 password=None,
				 suspended=None,
				 change_password=None,
				 org_unit=None
				):
		UserType.__init__(self, user_type)
		self.payload = {}

		self._set_org_unit(org_unit)
		if primary_email != None:
			self._set_primary_email(primary_email)

		if given_name and family_name:
			self.name = self._set_name(given_name, family_name)

		if password != None:
			self._set_password(password, self.change_password)

		if external_uid != None:
			self._set_external_uid(external_uid)

		if suspended != None:
			self._suspend_user(suspended)


	def _set_primary_email(self, primary_email):
		self.payload["primaryEmail"] = primary_email

	def _set_org_unit(self, org_unit):
		if org_unit == None:
			org_unit = self.default_org_unit

		self.payload["orgUnitPath"] = org_unit

	def _set_name(self, given_name, family_name):
		self.payload["name"] = {"givenName": given_name,
		 						"familyName": family_name}

	def _set_password(self, password, change_password):
		self.payload["password"] = password
		self._change_password(change_password)

	def _change_password(self, change_password):
		if change_password == None:
			self.payload["changePasswordAtNextLogin"] = self.change_password
		else:
			assert(type(change_password)) == bool
			self.payload["changePasswordAtNextLogin"] = change_password

	def _set_external_uid(self, user_id):
		self.payload["externalIds"] = [{"customType": self.external_uid_type,
								"type": "custom",
								"value": user_id}]

	def _suspend_user(self, suspended):
		if suspended != None:
			assert(type(suspended)) == bool
			self.payload["suspended"] = suspended


class GoogleJSON(BaseUser):
	"""This is a huge problem"""
	def __init__(self, data):
		assert(type(data)) == dict
		self.data = data
		user_type = 'staff'
		primary_email = data.get('primaryEmail')
		name = data.get('name') 
		given_name = name.get('givenName')
		family_name = name.get('familyName')
		uids = data.get('externalIds')
		if uids:
			external_uid = self.return_uid(uids)
		suspended = data.get('suspended')
		change_password = data.get('changePasswordAtNextLogin')
		org_unit = data.get('orgUnitPath')

		BaseUser.__init__(self, 
							user_type=user_type, 
							primary_email=primary_email,
							given_name=given_name,
							family_name=family_name,
							external_uid=external_uid,
							password=None,
							suspended=suspended,
							change_password=change_password,
							org_unit=org_unit)

	def return_uid(self, external_uids):
		for uid in external_uids:
				if uid.get('customType') in ('staff', 'student', 'employee'):
					return uid.get('value')


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


