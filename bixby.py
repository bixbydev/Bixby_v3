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

log.info('Starting Services')


ds = DirectoryService()
us = ds.users()



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

	



def main():
	all_users = paginate()
	for u in all_users:
		print u['primaryEmail'], u['orgUnitPath']



if __name__ == '__main__':
	pass
	# main()