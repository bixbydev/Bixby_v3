#!/usr/bin/env python

# Filename: util.py

#=====================================================================#
# Copyright (c) 2015 Bradley Hilton <bradleyhilton@bradleyhilton.com> #
# Distributed under the terms of the GNU GENERAL PUBLIC LICENSE V3.   #
#=====================================================================#

import base64
import getpass

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