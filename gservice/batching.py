#!/usr/bin/env python

# Filename: batching.py

#=====================================================================#
# Copyright (c) 2015 Bradley Hilton <bradleyhilton@bradleyhilton.com> #
# Distributed under the terms of the GNU GENERAL PUBLIC LICENSE V3.   #
#=====================================================================#

import database.mysql.base
from gservice.directoryservice import http, DirectoryService
from googleapiclient.http import BatchHttpRequest
from gservice import groups


con = database.mysql.base.CursorWrapper()
cursor = con.cursor


gm = groups.SchemaBuilder(groups.member_schema)

ds = groups.DirectoryService()
ms = ds.members()

# def return_user(request_id, response, exception):
#     print request_id
#     print response
#     if exception:
#         print exception


members_sql = """SELECT google_groupid
, google_userid
, primary_email
, group_email
FROM bv_group_member
WHERE group_email = 'technology@berkeley.net'"""

cursor.execute(members_sql)
members_list = cursor.fetchall()

members_dict = {}

for k, v in enumerate(members_list):
	members_dict[str(k)] = v


def return_user(request_id, response, exception):
	print '#' *25
	print request_id
	if exception:
		print exception
	else:
		gs.construct(**response)
		#print request_id
		print gs.db_payload




def batch_add(members):
	batch = BatchHttpRequest()
	for k, v in members.items()
		
		


def foo():
	# batch = BatchHttpRequest(callback=return_user) #specifies a callback for all responses
	batch = BatchHttpRequest()
	batch.add(ms.get(groupKey='0184mhaj2s0jvmu', memberKey='julietbonczkowski@berkeley.net'), callback=return_user)
	batch.add(ms.get(groupKey='0184mhaj2s0jvmu', memberKey='bradleyhilton@berkeley.net'), callback=return_user)
	batch.add(ms.get(groupKey='0184mhaj2s0jvmu', memberKey='jamesbrown@berkeley.net'), callback=return_user)

	batch.execute(http=http)
	

