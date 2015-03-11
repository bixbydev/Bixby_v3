#!/usr/bin/env python

# Filename: services.py

#=====================================================================#
# Copyright (c) 2015 Bradley Hilton <bradleyhilton@bradleyhilton.com> #
# Distributed under the terms of the GNU GENERAL PUBLIC LICENSE V3.   #
#=====================================================================#


# PyDocs https://google-api-client-libraries.appspot.com/documentation/admin/directory_v1/python/latest/admin_directory_v1.users.html
# The JSON request https://developers.google.com/admin-sdk/directory/v1/guides/manage-users#create_user
# This script https://developers.google.com/admin-sdk/directory/v1/quickstart/quickstart-python#step_2_install_the_google_client_library

from apiclient.discovery import build

from logger.log import log
from config import config
import auth

log.info('Starting The Bixby Log')


# This is the worker bee.
directory_service = build('admin', 'directory_v1', http=auth.http)


class GoogleUsers(object):
	# Maybe this should inharrit from apiclient.discovery
	def __init__(self):
		self.userservice = directory_service.users()



def TestGoogleUsersClass():
	# To-Do: Remove this test function
	gus = GoogleUsers()
	gus.userservice.list(customer='my_customer', domain=services.config.PRIMARY_DOMAIN).execute()
		


def paginate():
	"""This is going to be damn useful. I wonder if this is where a decorator would be handy?"""
	users = directory_service.users()
	params = {'customer': 'my_customer', 'domain': config.PRIMARY_DOMAIN }
	request = users.list(**params)
	all_pages = []
	pages = 0

	while (request != None ):
		pages = pages + 1
		print pages

		current_page = request.execute()

		all_pages.extend(current_page['users'])

		request = users.list_next(request, current_page)

	return all_pages



def main():
	all_users = paginate()
	for u in all_users:
		print u['primaryEmail'], u['orgUnitPath']



if __name__ == '__main__':
	pass
	# main()