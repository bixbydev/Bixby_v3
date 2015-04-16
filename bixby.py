#!/usr/bin/env python

# Filename: services.py

#=====================================================================#
# Copyright (c) 2015 Bradley Hilton <bradleyhilton@bradleyhilton.com> #
# Distributed under the terms of the GNU GENERAL PUBLIC LICENSE V3.   #
#=====================================================================#


# PyDocs https://google-api-client-libraries.appspot.com/documentation/admin/directory_v1/python/latest/admin_directory_v1.users.html
# The JSON request https://developers.google.com/admin-sdk/directory/v1/guides/manage-users#create_user
# This script https://developers.google.com/admin-sdk/directory/v1/quickstart/quickstart-python#step_2_install_the_google_client_library

from googleapiclient.model import makepatch

from logger.log import log
from config import config
from gservice.directoryservice import DirectoryService
from database import queries
import database.mysql.base
import database.oracle.base

log.info('Starting Bixby')


# ds = DirectoryService()
# us = ds.users()




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
	log.info('Truncating STUDENTS_PY Table')
	mycursor.cursor.execute('TRUNCATE TABLE STUDENTS_PY')
	mycursor.cursor.executemany(queries.insert_students_py, ps_students)


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




def paginate(service_object):
	"""This is going to be darn useful. I wonder if this is where a decorator 
		would be handy?"""
	params = {'customer': 'my_customer',
            'domain': config.PRIMARY_DOMAIN,
             'viewType': 'admin_view'}
	request = service_object.list(**params)
	all_pages = []
	pages = 0

	while (request != None ):
		pages = pages + 1
		print pages

		current_page = request.execute()

		all_pages.extend(current_page['users'])

		request = service_object.list_next(request, current_page)

	return all_pages

	



def test():
	all_users = paginate()
	for u in all_users:
		print u['primaryEmail'], u['orgUnitPath']


def main():
	mcon = database.mysql.base.CursorWrapper()
	mc = mcon.cursor

	ocon = database.oracle.base.CursorWrapper()
	oc = ocon.cursor

	refresh_staff_py(oc, mc)


if __name__ == '__main__':
	main()
	# main()