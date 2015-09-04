#!/usr/bin/env python

# Filename: util.py

#=====================================================================#
# Copyright (c) 2015 Bradley Hilton <bradleyhilton@bradleyhilton.com> #
# Distributed under the terms of the GNU GENERAL PUBLIC LICENSE V3.   #
#=====================================================================#

import os
import sys
import ssh
import traceback
import csv
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


def list_chunks(l, n):
    """Takes two arguments:
        l is a list
        and number, n
    Splits the list [l] into a list of lists with n items."""
    assert(type(l)) == list
    return [l[i:i+n] for i in range(0, len(l), n)]


def copy_foreign_table(source_cursor, source_query, destination_cursor, destination_table):
	"""Selects data in a foreign source database and inserts it into a table in the destination db
	The columns in the foreign and destination tables must match."""
	source_cursor.execute(source_query)
	columns_list = [i[0] for i in source_cursor.description]
	source_data = source_cursor.fetchall()
	insert = 'INSERT INTO %s \n(' %destination_table
	columns = ', '.join(columns_list) +') \n'
	values = 'VALUES ('+'%s, ' *(len(columns_list) - 1) +'%s)'
	insert_query = insert + columns + values
	print "Inserting : %s records" %source_cursor.rowcount
	destination_cursor.executemany(insert_query, source_data)


def insert_json_payload(table, payload, cursor=None):
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
	values += values
	if cursor is None:
		return sql, values
	else:
		cursor.execute(sql, values)


def csv_from_sql(query, file_output_path, file_name, cursor, header=True):
	full_file_path = os.path.join(file_output_path, file_name)
	f = open(full_file_path, 'wb')
	#log.info('Querying DB')
	cursor.execute(query)
	queryresults = cursor.fetchall()
	#log.info('Writing file: %s' % file_name)
	csvwriter = csv.writer(f, delimiter=',', quotechar='"',
							quoting=csv.QUOTE_MINIMAL)
	if header:
		csvwriter.writerow([i[0] for i in cursor.description])
	for row in queryresults:
		csvwriter.writerow(row)
	#print row
	f.close()


def sftp_file(username, password, hostname, local_path, remote_path, file_name):
	host_key_type = None 
	host_key = None

	try:
		host_keys = ssh.util.load_host_keys(os.path.expanduser('~/.ssh/known_hosts'))
	except IOError:
		try:
			# try ~/ssh/ too, because windows can't have a folder named ~/.ssh/
			host_keys = ssh.util.load_host_keys(os.path.expanduser('~/ssh/known_hosts'))
		except IOError:
			print "ERROR: Unable to open host keys file"
			host_keys={}

	if host_keys.has_key(hostname):
		host_key_type = host_keys[hostname].keys()[0]
		host_key = host_keys[hostname][host_key_type]
		print 'Using host key of type %s' % host_key_type

	try:
		transport = ssh.Transport((hostname))
		transport.connect(username=username, password=password, hostkey=host_key)
		sftp = ssh.SFTPClient.from_transport(transport)
		sftp.chdir(remote_path)
		sftp.put(local_path, file_name)
		transport.close()
	except Exception, e:
		print "Caught Exception: %s: %s" %(e.__class__, e)
		traceback.print_exc()
		try:
			transport.close()
		except:
			pass
		sys.exit(1)

