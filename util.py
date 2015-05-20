#!/usr/bin/env python

# Filename: util.py

#=====================================================================#
# Copyright (c) 2015 Bradley Hilton <bradleyhilton@bradleyhilton.com> #
# Distributed under the terms of the GNU GENERAL PUBLIC LICENSE V3.   #
#=====================================================================#

import base64
import getpass
from datetime import datetime
from config import config


def password_salt():
	global PASSWORD_SALT
	if not PASSWORD_SALT:
		salt = getpass.getpass('Enter the password salt: ')

	PASSWORD_SALT = salt
	return PASSWORD_SALT



def weak_encode(key, clear):
	enc = []
	for i in range(len(clear)):
		key_c = key[i % len(key)]
		enc_c = chr((ord(clear[i]) + ord(key_c)) % 256)
		enc.append(enc_c)
	return base64.urlsafe_b64encode("".join(enc))


def weak_decode(key, enc):
	dec = []
	enc = base64.urlsafe_b64decode(enc)
	for i in range(len(enc)):
		key_c = key[i % len(key)]
		dec_c = chr((256 + ord(enc[i]) - ord(key_c)) % 256)
		dec.append(dec_c)
	return "".join(dec)


def read_file_retrun_string(path_to_file):
	with open(path_to_file, 'rb') as f:
		return f.read()


def un_map(dic):
	"""
	Takes a dictionary and reverses the values to keys and keys to values.
	This only works for a dictionary with unique values.
	"""
	reverse_dic = {}
	for k, v in dic.iteritems():
		reverse_dic[v] = k

	return reverse_dic


def return_datetime(iso8601s):
	"""Returns datetime object from iso8601 string in Google JSON response"""
	try:
		return datetime.strptime(iso8601s, config.GTIME_FORMAT)
	except ValueError:
		"""Handles the Google Oauth Expiry Format"""
		return datetime.strptime(iso8601s, config.GEXPIRY_FORMAT)

def year_as_string():
	return datetime.datetime.strftime(datetime.datetime.now(), '%Y')


def json_date_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, datetime):
        serial = obj.isoformat()
        return serial


def chunks(li, n):
	"""Takes two arguments.
		a list, li
		lists n length, n
	Splits the list into a list of lists with n items."""
	return [li[i:i+n] for i in range(0, len(li), n)]

