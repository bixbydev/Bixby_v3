#!/usr/bin/env python

# Filename: users.py

#=====================================================================#
# Copyright (c) 2015 Bradley Hilton <bradleyhilton@bradleyhilton.com> #
# Distributed under the terms of the GNU GENERAL PUBLIC LICENSE V3.   #
#=====================================================================#


import json


class UserType(object):
	def __init__(self, user_type=None):
		if user_type == 'Staff':
			# TODO (bixbydev): Get domain from config
			domain = 'gtest.example.net'
		else:
			user_type = 'Student'
			domain = 'students.gtest.example.net'

		self.user_type = user_type
		self.domain = domain


class ExternalIds(UserType):
	def __init__(self, user_type):
		UserType.__init__(self, user_type)


class AppsUser(UserType):
	"""docstring for Users"""
	def __init__(self,
				 username, 
				 given_name, 
				 family_name, 
				 emails=[],
				 user_type=None,
				 external_userid=None, 
				 password=None,
				 suspended=False,
				 change_password=False
				 org_unit='/Disabled Users',
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


	def json_request(self):
		user_dict = {}
		user_dict['primaryEmail'] = self.full_email_address
		user_dict['name'] = {'givenName': self.given_name,
							'familyName': self.family_name}
		user_dict['suspended'] = self.suspended
		if self.password:
			user_dict['password'] = self.password
		user_dict['emails'] = {'address': self.full_email_address,
								'type': "work",
								'customType': "",
								'primary': True}
		user_dict['orgUnitPath'] = self.org_unit
		self.user_dict = user_dict

	def _add_googleid(self, googleid):




