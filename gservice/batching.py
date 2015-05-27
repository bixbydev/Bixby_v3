#!/usr/bin/env python

# Filename: batching.py

#=====================================================================#
# Copyright (c) 2015 Bradley Hilton <bradleyhilton@bradleyhilton.com> #
# Distributed under the terms of the GNU GENERAL PUBLIC LICENSE V3.   #
#=====================================================================#


from gservice.directoryservice import http, DirectoryService
from googleapiclient.http import BatchHttpRequest

def return_user(request_id, response, exception):
    print request_id
    print response
    if exception:
        print exception


ds = DirectoryService()
us = ds.users()

def foo():
	batch = BatchHttpRequest(callback=return_user) #specifies a callback for all responses
	batch.add(us.get(userKey='test1@example.net'), request_id='123')
	batch.add(us.get(userKey='test2@example.net'), request_id='1234')
	batch.add(us.get(userKey='test3@example.net'), request_id='5')

	batch.execute(http=http)