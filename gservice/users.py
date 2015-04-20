#!/usr/bin/env python

# Filename: users.py

#=====================================================================#
# Copyright (c) 2015 Bradley Hilton <bradleyhilton@bradleyhilton.com> #
# Distributed under the terms of the GNU GENERAL PUBLIC LICENSE V3.   #
#=====================================================================#


import json
from datetime import datetime
from googleapiclient.model import makepatch
from config import config
from database.mysql.base import CursorWrapper
from database import queries


STAFF_DOMAIN = config.STAFF_DOMAIN
STUDENT_DOMAIN = config.STUDENT_DOMAIN

GTIME_FORMAT = '%Y-%m-%dT%H:%M:%S.000Z'
GEXPIRY_FORMAT = '%Y-%m-%dT%H:%M:%SZ'


bixby_user_map = {'bixbyId': 'ID',
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
		return datetime.strptime(iso8601s, GTIME_FORMAT)
	except ValueError:
		"""Handles the Google Oauth Expiry Format"""
		return datetime.strptime(iso8601s, GEXPIRY_FORMAT)


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
		self.user_id = user_id
		self.payload["externalIds"] = [{"customType": self.external_uid_type,
								"type": "custom",
								"value": self.user_id}]

	def _suspend_user(self, suspended):
		if suspended != None:
			assert(type(suspended)) == bool
			self.payload["suspended"] = suspended


class GoogleJSON(BaseUser):
	"""This might be a problem"""
	domain_to_usertype = config.DOMAIN_TO_USERTYPE_MAP
	def __init__(self, data):
		assert(type(data)) == dict
		self.data = data
		primary_email = data.get('primaryEmail')
		user_type = self._user_type_from_email(primary_email)
		name = data.get('name') 
		given_name = name.get('givenName')
		family_name = name.get('familyName')
		uids = data.get('externalIds')
		if uids:
			external_uid = self.return_uid(uids)
		else:
			external_uid = None
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

	def _user_type_from_email(self, primary_email):
		domain = primary_email.split('@')[1]
		return self.domain_to_usertype[domain]


class BixbyUser(BaseUser, CursorWrapper):
	def __init__(self):
		CursorWrapper.__init__(self)

	def _check_user_exists(self, primary_email):
		self.cursor.execute(queries.sql_get_bixby_user, (primary_email,))
		bixby_user = self.cursor.fetchone()
		if bixby_user:
			self.user_in_db = True
			self.bixby_id = bixby_user[0]
			print bixby_user
			self._db_user_object(user_type = bixby_user[1],
								primary_email = bixby_user[2],
								given_name = bixby_user[3],
								family_name = bixby_user[4],
								external_uid = bixby_user[5],
								suspended = bool(bixby_user[6]),
								change_password = bool(bixby_user[7]),
								org_unit = bixby_user[9]
								)
		else:
			self.user_in_db = False

	def _db_user_object(self, 
					user_type,
					primary_email,
					given_name,
					family_name,
					external_uid,
					suspended,
					change_password,
					org_unit):
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
		
	def _insert_new_user(self, google_data):
		insert = {}
		# for k in google_data.payload.iterkeys():
		# 	db_key = bixby_user_map.get(k)
		# 	d[db_key] = google_data.payload[k]

		# for k in google_data.data.iterkeys():
		# 	db_key = bixby_user_map.get(k)
		# 	if db_key:
		# 		insert[db_key] = google_data.data[k]

		insert['USER_TYPE'] = google_data.user_type
		insert['FIRST_NAME'] = google_data.data.get('name').get('givenName')
		insert['LAST_NAME'] = google_data.data.get('name').get('familyName')
		insert['PRIMARY_EMAIL'] = google_data.data.get('primaryEmail')
		insert['GOOGLE_ID'] = google_data.data.get('id')
		insert['LASTLOGIN_DATE'] = return_datetime(google_data.data.get('lastLoginTime'))
		insert['GLOBAL_ADDRESSBOOK'] = int(google_data.data.get('includeInGlobalAddressList'))
		insert['CREATION_DATE'] = return_datetime(google_data.data.get('creationTime'))
		insert['ETAG'] = google_data.data.get('etag')
		insert['SUSPENDED'] = int(google_data.data.get('suspended'))
		insert['CHANGE_PASSWORD'] = int(google_data.data.get('changePasswordAtNextLogin'))
		insert['OU_PATH'] = google_data.data.get('orgUnitPath')

		if google_data.payload.get('externalIds') == None:
			uid = self.__get_uid_from_lookuptable(google_data.data.get('primaryEmail'))
			if uid:
				google_data._set_external_uid(uid) # this will go in payload
				# google_data.payload['externalIds']
				insert['EXTERNAL_UID'] = uid
		
		self.__insert_dict('bixby_user', insert)

	def __get_uid_from_lookuptable(self, primary_email):
		self.cursor.execute(queries.lookup_external_uid, (primary_email,))
		uid = self.cursor.fetchone()
		if uid:
			return uid[0]
		else:
			return uid


	def load_bixby_db_from_google_json(self, json_object):
		google_object = GoogleJSON(json_object)
		self._check_user_exists(google_object.payload['primaryEmail'])
		if self.user_in_db == False:
			print "I would insert this object", google_object.payload['primaryEmail']
			
			return self._insert_new_user(google_object)

		else:
			print makepatch(google_object.payload, self.payload)
			
	def __insert_dict(self, table, dictionary):
		places = ', '.join(['%s'] * len(dictionary))
		columns = ', '.join(dictionary.keys())
		sql = """INSERT INTO %s (%s) VALUES (%s)""" %(table, columns, places)
		self.cursor.execute(sql, dictionary.values())
		#print sql %dictionary.values()



def foo(users_objects):
	bu = BixbyUser()
	for user in users_objects:
		bu.load_bixby_db_from_google_json(user)





