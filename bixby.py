#!/usr/bin/env python

# Filename: services.py

#=====================================================================#
# Copyright (c) 2015 Bradley Hilton <bradleyhilton@bradleyhilton.com> #
# Distributed under the terms of the GNU GENERAL PUBLIC LICENSE V3.   #
#=====================================================================#


# PyDocs https://google-api-client-libraries.appspot.com/documentation/admin/directory_v1/python/latest/admin_directory_v1.users.html
# The JSON request https://developers.google.com/admin-sdk/directory/v1/guides/manage-users#create_user
# This script https://developers.google.com/admin-sdk/directory/v1/quickstart/quickstart-python#step_2_install_the_google_client_library


import json
import csv
import sys
from logger.log import log
from googleapiclient.model import makepatch

from config import config
from gservice.directoryservice import DirectoryService
from gservice import users
from database import queries
import database.mysql.base
import database.oracle.base
from config import config
import groups.schoolconferences
import groups.sectiongroups
import groups.yoggroups


log.info('Starting Bixby')

def refresh_staff_py(oracursor, mycursor):
	"""Pulls the staff data from PowerSchool and copies it to Bixby DB
	Eventually replace this with something more system independent BEH"""
	log.info('Refreshing STAFF_PY table data')
	ps_staff = oracursor.execute(queries.get_staff_from_sis)
	ps_staff = oracursor.fetchall()
	log.info('Truncating STAFF_PY Table')
	mycursor.execute('TRUNCATE TABLE STAFF_PY')
	mycursor.executemany(queries.insert_staff_py, ps_staff)
	log.info('%s Records Inserted' %oracursor.rowcount)


def refresh_students_py(oracursor, mycursor):
	"""Pulls the student data from PowerSchool and copies it to Bixby DB
	Eventually replace this with something more system independent BEH"""
	log.info('Refreshing STUDENTS_PY table data')
	ps_students = oracursor.execute(queries.get_students_from_sis)
	ps_students = oracursor.fetchall()
	print sys.getsizeof(ps_students)
	log.info('Truncating STUDENTS_PY Table')
	mycursor.execute('TRUNCATE TABLE STUDENTS_PY')
	log.info('Table Truncated')
	mycursor.executemany(queries.insert_students_py, ps_students)
	log.info('%s Records Inserted' %oracursor.rowcount)


def multidb_bulk_insert(sourcecursor, destinationcursor, sourcequery, destinationtable):
	"""Takes two cursors connected to two different (or the same) database and creates an
	INSERT query from the source query and inserts the source data rows into the destination
	database. The column names from the source query MUST mach the column names in the
	destination table."""
	scursor = sourcecursor
	dcursor = destinationcursor
	scursor.execute(sourcequery)
	dest_col_names = [i[0] for i in scursor.description]
	dest_query_col_values = '%s, '*(len(dest_col_names) - 1) + '%s'
	dest_query_col_names = ', '.join(dest_col_names)
	srow_data = scursor.fetchall()
	dest_query = """INSERT INTO %s (%s) VALUES""" %(destinationtable, dest_query_col_names)
	dest_query = dest_query + '( %s)'
	dest_query = dest_query % dest_query_col_values
	log.info('Inserting %s Into Table: %s' %(scursor.rowcount, destinationtable) )
	log.debug(dest_query)
	dcursor.executemany(dest_query, srow_data)


def paginate(service_object, **kwargs):
	"""This function is redundant. Should be referenced from directoryservice"""
	params = {'customer': 'my_customer',
				'domain': config.PRIMARY_DOMAIN,
				'viewType': 'admin_view'}
	request = service_object.list(**kwargs)
	all_pages = []
	pages = 0

	while (request != None ):
		pages = pages + 1
		print pages

		current_page = request.execute()

		# will not work for groups because of this
		all_pages.extend(current_page['users'])

		request = service_object.list_next(request, current_page)

	return all_pages


def dump_all_users_json(file_path=None):
	"""Dumps all users from Google Domain to a JSON File"""
	ds = DirectoryService()
	us = ds.users()
	all_users_json = paginate(us, projection='full', customer='my_customer', viewType='admin_view')
	with open(file_path, 'wb') as f:
		f.write(json.dumps(all_users_json, indent=4))


def refresh_all_google_users(cursor=None, user_service=None):
	file_path = config.ALL_USERS_JSON
	with open(file_path, 'rb') as f:
		all_users_json = json.loads(f.read())

	for google_user in all_users_json:
		#if google_user.get('externalIds'):
		db_payload = users.update_from_json_object(google_user)
		primary_email  = google_user.get('primaryEmail')
		google_id = google_user.get('id')
		
		if cursor is None:
			print primary_email, google_id
		#users.update_bixby_user_from_dictionary(cursor, google_id, db_payload)
		insert_json_payload(cursor, 'bixby_user', db_payload)



def insert_json_payload(cursor, table, payload):
	""" Usage
	cursor: is a cursor object to the db to insert/update
	table: the name of the database table being updated 
	payload: a json object/dictionary of key/value pairs. 
		Keys must be column names
		Values must be valid data types
		It's not perfect, but it saves time.
	"""
	places = ', '.join(['%s'] * len(payload))
	col_names = payload.keys()
	columns = ', '.join(col_names)
	values = payload.values()
	update_cols = ', '.join([i+"=%s" for i in col_names])
	sql = """INSERT INTO %s (%s) VALUES (%s)
				ON DUPLICATE KEY 
					UPDATE %s\n""" %(table, columns, places, update_cols)
	log.info('Inserted User to Group: %s' %str(values))
	values += values
	log.debug((sql) %tuple(values))
	if cursor is None:
		print sql %tuple(values)
	else:
		cursor.execute(sql, values)



def sync_all_users_from_google(cursor=None, user_service=None):
	"""This function relies on the lookuptable csv file. It was written to sync
	users for the first run and update their internal userid
	This function is being replaces by refresh_all_google_users"""
	file_path = config.ALL_USERS_JSON
	with open(file_path, 'rb') as f:
		all_users_json = json.loads(f.read())

	lookup_table = build_uid_lookup(file_path=config.LOOKUP_TABLE_CSV)
	
	for google_user in all_users_json:
		original_user = users.update_from_json_object(google_user)
		primary_email = original_user.get('PRIMARY_EMAIL')
		user_key = primary_email
		external_uid = lookup_table.get(user_key)

		if external_uid:
			q_email = 'SELECT EXTERNAL_UID FROM bixby_user WHERE PRIMARY_EMAIL = %s'
			cursor.execute(q_email, (primary_email,))
			uid_in_bixby = cursor.fetchone() 
			if uid_in_bixby is None:
				update_user = original_user.copy() 
				update_user.update(external_uid)

				if cursor is None:
					print update_user

				else:
					users.insert_user_from_dictionary(cursor, 'bixby_user', update_user)
				
					patch = {"externalIds": [
							{ "customType": external_uid.get('USER_TYPE'), 
							"type": "custom", 
							"value": external_uid.get('EXTERNAL_UID') }
						]
					 }
					if patch and user_service:
						user_service.patch(userKey=user_key, body=patch).execute()
			else:
				log.debug('User Skipped: %s' %primary_email)



def build_uid_lookup(file_path=None):
	"""Reads the lookup file and builds the lookuptable that maps the external_uid
	to the users email which is essential to making bixby function"""
	lookup_dict = {}
	with open(file_path, 'rU') as f:
		reader = csv.reader(f, delimiter=',')
		for row in reader:
			lookup_dict[row[3]] = {'PRIMARY_EMAIL': row[3], 
															'EXTERNAL_UID': row[4], 
															'USER_TYPE': row[0]}
	return lookup_dict 


def refresh_users(users_list):
	"""Takes a list of tuples with external_uid and user_type"""
	# TODO (bixbydev): validate the list
	bu = users.BixbyUser()
	for external_uid, user_type in users_list:
		# print external_uid, user_type
		bu.init_user(external_uid, user_type)


def current_users(cursor, user_type=None, random=False, limit=None):
	params = []
	sql = """SELECT EXTERNAL_UID, USER_TYPE
					FROM bixby_user
					"""
	if user_type in ('staff', 'student'):
		sql += '\nWHERE USER_TYPE = %s'
		params.append(user_type)
	else:
		sql += '\nWHERE USER_TYPE IS NOT NULL'

	if random == True:
		sql += '\nORDER BY RAND()'

	if limit:
		assert(type(limit)) == int
		sql += "\nLIMIT %s"
		params.append(limit)

	cursor.execute(sql, params)
	users = cursor.fetchall()
	return users

def new_staff_and_students(cursor):
	cursor.execute(queries.new_staff_and_students)
	# cursor.execute(queries.new_staff_only)
	new_users = cursor.fetchall()
	return new_users


mcon = database.mysql.base.CursorWrapper()
mc = mcon.cursor

def main():
	ocon = database.oracle.base.CursorWrapper()
	oc = ocon.cursor

	ds = DirectoryService()
	us = ds.users()

	refresh_staff_py(oc, mc)
	refresh_students_py(oc, mc)
	#dump_all_users_json(file_path=config.ALL_USERS_JSON)
	#sync_all_users_from_google(cursor=mc, user_service=us)
	log.info('Updating Users')
	users_list = current_users(mc, user_type='student', random=False)
	refresh_users(users_list)
	log.info('Adding New Users')
	new_users = new_staff_and_students(mc)
	refresh_users(new_users)

	# Run the School Conferences
	groups.schoolconferences.main()

	# Run the Section Groups
	groups.sectiongroups.main()
	
	# Run the Year of Graduation (YOG) Groups
	groups.yoggroups.main()



if __name__ == '__main__':
	main()


