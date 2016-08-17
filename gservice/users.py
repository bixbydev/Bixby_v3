#!/usr/bin/env python

# Filename: users.py

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


bixby_user_map = {'bixbyId': 'ID',
			 'suspended': 'SUSPENDED',
			 'primaryEmail': 'PRIMARY_EMAIL',
			 'creationTime': 'CREATION_TIME',
			 'id': 'GOOGLE_ID',
			 'includeInGlobalAddressList': 'GLOBAL_ADDRESSBOOK',
			 'lastLoginTime': 'LASTLOGIN_DATE',
			 'familyName': 'FAMILY_NAME',
			 'givenName': 'GIVEN_NAME',
			 'orgUnitPath': 'OU_PATH',
			 'orgUnitId': 'GOOGLE_OUID',
			 'changePasswordAtNextLogin': 'CHANGE_PASSWORD',
			 'externalIds': 'EXTERNAL_UID',
			 'etag': 'ETAG'}

json_to_columns_map = {'suspended': 'SUSPENDED',
			 'primaryEmail': 'PRIMARY_EMAIL',
			 'creationTime': 'CREATION_TIME',
			 'id': 'GOOGLE_ID',
			 'includeInGlobalAddressList': 'GLOBAL_ADDRESSBOOK',
			 'lastLoginTime': 'LASTLOGIN_DATE',
			 'familyName': 'FAMILY_NAME',
			 'givenName': 'GIVEN_NAME',
			 'orgUnitPath': 'OU_PATH',
			 'orgUnitId': 'GOOGLE_OUID',
			 'changePasswordAtNextLogin': 'CHANGE_PASSWORD',
			 'externalIds': 'EXTERNAL_UID',
			 'etag': 'ETAG',
			 'customType': 'USER_TYPE',
			 'value': 'EXTERNAL_UID'}


columns_to_json_map = un_map(json_to_columns_map)

reverse_user_map = un_map(bixby_user_map)

class Error(Exception):
	"""Base module for Exception in this module"""


class BadUserName(Error):
	"""The Username is Already in Use"""


class UserType(object):
	user_type_map = config.USER_TYPE_MAP
	def __init__(self, user_type=None):
		self.user_type = user_type
		self.domain = self.user_type_map[user_type]['domain']
		self.user_type_id = self.user_type_map[user_type]['userTypeId']
		self.default_ou_path = self.user_type_map[user_type]['defaultOU']
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
				 ou_path=None,
				 google_ouid=None,
				 global_addressbook=None,
				 creation_time=None,
				 **kwargs
				):
		UserType.__init__(self, user_type)
		self.payload = {}

		self._set_ou_path(ou_path)

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

		if global_addressbook != None:
			self._set_include_in_global_addressbook(global_addressbook)

		if creation_time != None:
			if type(creation_time) == datetime.datetime:
				creation_time = datetime.isoformat(creation_time)

			self._set_creation_time(creation_time)

		if google_ouid != None:
			self._set_orgunit_id(google_ouid)


	def _set_primary_email(self, primary_email):
		self.payload["primaryEmail"] = primary_email

	def _set_ou_path(self, ou_path):
		if ou_path == None:
			ou_path = self.default_ou_path

		self.payload["orgUnitPath"] = ou_path

	def _set_orgunit_id(self, google_ouid):
		if google_ouid == None:
			raise Exception('No google_ouid')

		self.payload['orgUnitId'] = google_ouid

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
			assert(type(bool(change_password))) == bool
			self.payload["changePasswordAtNextLogin"] = bool(change_password)

	def _set_external_uid(self, user_id):
		self.user_id = user_id
		self.payload["externalIds"] = [{"customType": self.external_uid_type,
								"type": "custom",
								"value": self.user_id}]

	def _suspend_user(self, suspended):
		if suspended != None:
			assert(type(bool(suspended))) == bool
			self.payload["suspended"] = bool(suspended)

	def _set_include_in_global_addressbook(self, global_addressbook):
		assert(type(bool(global_addressbook))) == bool
		self.payload['includeInGlobalAddressList'] = bool(global_addressbook)

	def _set_creation_time(self, creation_time):
		self.creation_time = creation_time
		self.payload['creationTime'] = creation_time


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
		ou_path = data.get('orgUnitPath')

		BaseUser.__init__(self, 
							user_type=user_type, 
							primary_email=primary_email,
							given_name=given_name,
							family_name=family_name,
							external_uid=external_uid,
							password=None,
							suspended=suspended,
							change_password=change_password,
							ou_path=ou_path)

	def return_uid(self, external_uids):
		for uid in external_uids:
				if uid.get('customType') in ('staff', 'student', 'employee'):
					return uid.get('value')

	def _user_type_from_email(self, primary_email):
		domain = primary_email.split('@')[1]
		return self.domain_to_usertype[domain]





class UserMod(BaseUser, CursorWrapper):
	def __init__(self, user, key_type='id'):
		"""Usage: 
			user: the google id, bixby_id, or primary_email of the user
			key_type: id or email
		"""
		CursorWrapper.__init__(self)
		self.user = user
		assert(key_type) in ('id', 'email', 'bixby')
		if key_type == 'id': # Sets the User DB Key for accessing record
			self._db_key = 'GOOGLE_ID'

		elif key_type == 'email':
			self._db_key = 'PRIMARY_EMAIL'

		elif key_type == 'bixby': 
			# This could be problematic if bixby_id and google_id overlap
			self._db_key = 'ID'

		else:
			raise Exception('Invalid Key Type: %' %key_type)

		valid_user = self.__get_user_key()

	def __get_user_key(self):
		valid_user_sql = """SELECT ID, GOOGLE_ID, PRIMARY_EMAIL 
							FROM bixby_user
							WHERE %s""" %self._db_key
		valid_user_sql += """ = %s"""
		self.cursor.execute(valid_user_sql, (self.user,))
		identifiers = self.cursor.fetchone()
		if identifiers:
			self.bixby_id, self.user_key, self.primary_email = identifiers
			return True

	def _get_stored_user_obj(self):
		pass
		








class BixbyUser(BaseUser, CursorWrapper, DirectoryService):
	def __init__(self):
		CursorWrapper.__init__(self)
		DirectoryService.__init__(self)
		self.uservice = self.directory_service.users()

	def init_user(self, external_uid, user_type):
		"""Initialize User"""
		self.external_uid = external_uid
		self.user_type = user_type
		self.existng_user = self._is_existing_user(external_uid, user_type)
		if self.existng_user:
			self.__get_bixby_id(external_uid, user_type)
			self.user_key = self.__get_user_key()
			user_object_from_bixby = self._get_user_object_by_id(self.bixby_id)
			user_object_from_py = self._get_user_object_from_py_table(self.external_uid, self.user_type) 
			username = self.bu.payload.get('primaryEmail')
			if user_object_from_py.get('ERROR'):
				patch = None
				log.debug(user_object_from_py.get('ERROR'))
			else:
				patch = makepatch(user_object_from_bixby, user_object_from_py)

			if patch:
				log.info('Patching user %s %s with patch %s' 
										%(username,
											self.bixby_id, 
											str(patch)))
				patch_result = self.uservice.patch(userKey=self.user_key,
													 body=patch).execute()
				log.info('PATCH RESULT: '+json.dumps(patch_result, indent=4))
				update_object = update_from_json_object(patch_result)
				update_user_from_dictionary(self.cursor, self.bixby_id, update_object)
				# time.sleep(1)

			else:
				log.debug("""No Change Skippng User: %s, %s""" 
										%(self.external_uid, self.user_type))
		else:
			#  # What was this for?
			new_user_object = self._get_new_user_object(external_uid, user_type)
			if new_user_object.get('suspended') == False:
				self.new_user(external_uid, user_type)
				log.info("Creating User %s" %self.new_username)
				new_user_object['primaryEmail'] = self.new_username
				insert_result = self.uservice.insert(body=new_user_object).execute()
				insert = update_from_json_object(insert_result)
				log.info("""Created User:  %s""" %json.dumps(insert_result))
				insert_user_from_dictionary(self.cursor, 'bixby_user', insert)

			else:
				# TODO (bixbydev): Something must be done about this. It's a hack.
				log.debug('Skipping suspended user')
				if new_user_object.get('ERROR'):
					log.warn(new_user_object.get('ERROR'))

	def new_user(self, external_uid, user_type):
		try:
				self.new_username = unique_username(self.cursor, external_uid, user_type)

		except BadUserName, e:
			log.warn('Could Not Create Account for %s, %s, %s' 
							(external_uid, user_type, e) )


	def _is_existing_user(self, external_uid, user_type):
		"""Something """
		self.cursor.execute(queries.get_bixby_id, (external_uid, user_type))
		bixby_user = self.cursor.fetchone()
		if bixby_user:
			return True
		else:
			return False

	def _generate_username(self, external_uid, user_type):
		pass

	def __get_bixby_id(self, external_uid, user_type):
		self.cursor.execute(queries.get_bixby_id, (external_uid, user_type))
		bixby_user = self.cursor.fetchone()
		self.bixby_id = bixby_user[0]

	def __get_user_key(self):
		self.cursor.execute(queries.get_user_key, (self.bixby_id,))
		return self.cursor.fetchone()[0]

	def _get_user_object_by_id(self, bixby_id):
		"""Looks in the bixby_user table and returns a google object"""
		params = """WHERE bu.id = %s"""
		q = queries.get_userinfo_vary_params %params
		self.cursor.execute(q, (bixby_id,))
		self.columns = [ i[0] for i in self.cursor.description ]
		self.values = self.cursor.fetchone()
		d = {}
		d['user_type'] = self.user_type
		for k, v in zip(self.columns, self.values):
			k = k.lower()
			d[k] = v

		self.bu = BaseUser(**d)
		return self.bu.payload

	def _get_new_user_object(self, external_uid, user_type):
		# This is redundant but I need to make this work so this is redundant
		if user_type == 'staff':
			q = queries.get_new_staff_py
			password = 'BerkeleyStaff'+config.YEAR
			change_password = True

		elif user_type == 'student':
			q =  queries.get_new_student_py
			self.cursor.execute(queries.get_student_number, (external_uid,))
			password = self.cursor.fetchone()[0]
			password = 'Berkeley'+str(password)
			change_password = False

		else:
			q = None

		self.cursor.execute(q, (external_uid,))
		columns = [ i[0] for i in self.cursor.description ]
		values = self.cursor.fetchone()
		if values:
			d = {}
			d['user_type'] = user_type
			for k, v in zip(columns, values):
				k = k.lower()
				d[k] = v
			self.sp = BaseUser(**d)
			# Add the new password
			self.sp.payload['password'] = password
			self.sp.payload['change_password'] = change_password
			return self.sp.payload
		else:
			# This is so wrong. There are a ton of users who will keep triggering
			# this until it is fixed.
			error_string = 'INVALID USER UID: %s, Type: %s' %(external_uid, user_type)
			return {'ERROR': error_string }

	def _get_user_object_from_py_table(self, external_uid, user_type):
		"""Looks in the staf_py table and returns a google object"""
		# This is redundant but I need to make this work so this is redundant
		if user_type == 'staff':
			q = queries.sql_get_staff_py
			domain = 'berkeley.net'

		elif user_type == 'student':
			q = queries.sql_get_students_py
			domain = 'students.berkeley.net'

		else:
			q = None

		self.cursor.execute(q, (external_uid, user_type))
		columns = [ i[0] for i in self.cursor.description ]
		values = self.cursor.fetchone()
		if values:
			d = {}
			d['user_type'] = user_type
			for k, v in zip(columns, values):
				k = k.lower()
				d[k] = v
			# Breakup the username in case no domain is associated
			# TODO (bixbydev): use the domain from the UserType Class
			username = d.get('primary_email').split('@')
			if len(username) == 1:
				d['primary_email'] = username[0]+'@'+domain
				log.debug('Fixed Up Username %s' %(username[0]+'@'+domain,) )

			self.sp = BaseUser(**d)

		else:
			error_string = 'NON-MANAGED UID: %s Type: %s' %(external_uid, user_type)
			return {'ERROR': error_string } 
		
		return self.sp.payload


def get_external_uid_from_json(uid_list):
	"""
	returns the external_uid out of the google json object as a dictionary
	with bixby_user columns as keys
	"""
	for obj in uid_list:
		utype = obj.get('customType', None)
		if utype in ('student', 'staff'):
			d = {'USER_TYPE': utype, 'EXTERNAL_UID': obj.get('value')}
			return d
		else:
			pass


def update_from_json_object(json_object):
	"""Returns a dictionary of user info with column names as keys"""
	d = {}
	for key, value in json_object.iteritems():
		column = json_to_columns_map.get(key, None)
		if type(value) == dict:
			#look recursivly down the dict
			d.update(update_from_json_object(value))  
		elif column is None:
			pass
		else:
			if key == 'externalIds':
				uid = get_external_uid_from_json(value)
				if uid is not None: d.update(uid)

			elif key in ('lastLoginTime', 'creationTime'):
				d[column] = return_datetime(value)
			else: 
				d[column] = value
	return d


def insert_user_from_dictionary(cursor, table, dictionary):
	places = ', '.join(['%s'] * len(dictionary))
	columns = ', '.join(dictionary.keys())
	sql = """INSERT INTO %s (%s) VALUES (%s)""" %(table, columns, places)
	log.debug(sql)
	cursor.execute(sql, dictionary.values())
	log.info('Inserting Record %s' %json.dumps(dictionary, default=json_date_serial))


def update_user_from_dictionary(cursor, bixby_id, dictionary):
	update = 'UPDATE bixby_user SET {}'
	columns = update.format(', '.join('{}=%s'.format(k) for k in dictionary))
	where = ' WHERE id = %s' %bixby_id
	sql = columns + where
	log.debug(sql)
	cursor.execute(sql, dictionary.values())
	log.info('Updateing User %s Record %s' %(bixby_id, str(dictionary) ))


def update_bixby_user_from_dictionary(cursor, google_id, dictionary):
	"""Updates bixby_user table using the google supplied ID as the key"""
	update = 'UPDATE bixby_user SET {}'
	columns = update.format(', '.join('{}=%s'.format(k) for k in dictionary))
	where = ' WHERE google_id = %s' %google_id
	sql = columns + where
	log.debug(sql)
	if cursor is None:
		print sql
	else:
		cursor.execute(sql, dictionary.values())
		log.info('Updateing User %s Record %s' %(google_id, str(dictionary) ))


def sanatize_username(username_string):
	"""Remove special charactors from usernames"""
	return username_string.translate(None, "\' -;@#$%!.,/").lower()


def primary_email_exists(cursor, primary_email, domain):
	primary_email = primary_email.split('@')[0]
	primary_email = primary_email+'@'+domain
	one_user = """SELECT PRIMARY_EMAIL 
				FROM bixby_user 
				WHERE PRIMARY_EMAIL = %s"""
	cursor.execute(one_user, (primary_email,))
	if cursor.fetchone():
		return True
	else:
		return False


# Generate an unique username within the domain!
def unique_username(cursor, external_uid, user_type):
	# Check for Username Changes/Manual Username
	new_uname = None
	try_uname = None
	bad_overide = False
	ut = UserType(user_type)
	domain = ut.domain 
	if ut.user_type == 'student':
		lookup_sql = queries.get_new_student_py

	elif ut.user_type == 'staff':
		lookup_sql = queries.get_new_staff_py

	cursor.execute(lookup_sql, (external_uid,))
	info = cursor.fetchone()
	email_override_address = info[1]
	given_name = info[2]
	family_name = info[3]
	middle_name = info[4]

	if email_override_address != None:
		email_override_address = email_override_address.split('@')[0]
		new_uname = sanatize_username(email_override_address)
		bad_overide = True
	else:
		new_uname = sanatize_username(given_name+family_name)

	#Determine true/false username exists
	user_exists = primary_email_exists(cursor, new_uname, domain)

	if user_exists and bad_overide == True:
		raise BadUserName('The override username exists external_uid: %s %s', 
							external_uid, user_type)
	
	# Try creating a username with the middle initial
	elif user_exists and middle_name != None:
		log.info("Duplicate Username Avoided %s" %new_uname)
		new_uname = sanatize_username(given_name+middle_name[0]+family_name)
		user_exists = primary_email_exists(cursor, new_uname, domain)

	else:
		pass
	
	unique = 1
	while user_exists == True:
		user_exists = primary_email_exists(cursor, new_uname, domain)
		try_uname = new_uname+str(unique)
		user_exists = primary_email_exists(cursor, try_uname, domain)
		unique = unique + 1
		log.info("Duplicate Username Avoided %s" %try_uname)
	
	if try_uname:
		new_uname = try_uname

	return new_uname+'@'+domain



def refresh_bixby_from_google(users_objects):
	bu = BixbyUser()
	for user in users_objects:
		bu.load_bixby_db_from_google_json(user)


def patch_user(primary_email, original, modified, users_service):
	""" Check for new records that qualify for a new account.
			Generate the accounts.
			Write the accounts to Google.
			Record them in the DB.

		Go through the list of current accounts.
			Determine what has changed.
			Patch the changes.
			Write changes back to DB.

	Ocasionally Check for users in Google that don't exist in bixby_user
	"""
	pass


