#!/usr/bin/env python

# Filename: 

#=====================================================================#
# Copyright (c) 2015 Bradley Hilton <bradleyhilton@bradleyhilton.com> #
# Distributed under the terms of the GNU GENERAL PUBLIC LICENSE V3.   #
#=====================================================================#

# Example Use: cd.list(customerId='my_customer', query='location: Thousand Oaks').execute()
# https://support.google.com/chrome/a/answer/1698333?hl=en

import json
import csv
import time

import util
import gservice.directoryservice

ds = gservice.directoryservice.DirectoryService()
cd = ds.chromeosdevices()

# device_lookup = {}

class Devices(object):
	"""docstring for ClassName"""
	def __init__(self):
		self.ds = gservice.directoryservice.DirectoryService()
		self.cd = ds.chromeosdevices()
		self.all_devices = None
		self.fetched_devices = None
		self._projection = 'BASIC'
		self._query = None
		self._device_lookup = self._build_devicelookup()

	def _build_devicelookup(self):
		device_lookup = {}
		self.all_devices = self._get_devices()
		for i in self.all_devices:
			serial = i.get('serialNumber')
			deviceid = i.get('deviceId')
			device_lookup[serial] = deviceid
		return device_lookup

	def _get_devices(self):
		return gservice.directoryservice.paginate(self.cd, 
			method='chromeosdevices', customerId='my_customer', 
			projection=self._projection, query=self._query)

	def _get_deviceid(self, deviceserial):
		return self._device_lookup[deviceserial]

	def update_device(self, deviceserial, user=None, assetid=None, location=None, 
		notes=None, orgunitpath=None):
		"""
		user - the user account associated with the enrolled device
		assetid - the asset tracking number assigned to the device
		location - the primary location of the device or name of the chromecart
		orgunitpath - the full path of the organization unit where the device is 
			enrolled
		"""
		patchbody = {}

		if not self._device_lookup:
			self._build_devicelookup()

		deviceid = self._get_deviceid(deviceserial)

		if user:
			patchbody['annotatedUser'] = user

		if assetid:
			patchbody['annotatedAssetId'] = assetid

		if location:
			patchbody['annotatedLocation'] = location

		if notes:
			patchbody['notes'] = notes

		if orgunitpath:
			patchbody['orgUnitPath'] = orgunitpath

		if patchbody:
			devicepatch = self.cd.patch(customerId='my_customer', 
				deviceId=deviceid, body=patchbody)
			print patchbody
			return devicepatch.execute()

	def dump_json(self, file_path):
		if not self.fetched_devices:
			self._projection = 'FULL'
			self.fetched_devices = self._get_devices()
		with open(file_path, 'wb') as f:
			f.write(json.dumps(self.fetched_devices, indent=4))

	def dump_csv(self, file_path):
		if not self.fetched_devices:
			self._projection = 'FULL'
			self.fetched_devices = self._get_devices()
		with open(file_path, 'wb') as f:
			csvwriter = csv.writer(f, delimiter=',', quotechar="\"", 
								quoting=csv.QUOTE_MINIMAL)
			csvwriter.writerow(['serialNumber', 'deviceId', 'macAddress',
				'annotatedAssetId', 'annotatedLocation', 'annotatedUser',
				'notes', 'orgUnitPath', 'model', 'platformVersion', 'lastSync', 'status'])
			for i in self.fetched_devices:
				serialNumber = i.get('serialNumber')
				deviceId = i.get('deviceId')
				macAddress = i.get('macAddress')
				annotatedAssetId = i.get('annotatedAssetId')
				annotatedLocation = i.get('annotatedLocation')
				annotatedUser = i.get('annotatedUser')
				orgUnitPath = i.get('orgUnitPath')
				notes = i.get('notes')
				model = i.get('model')
				platformVersion = i.get('platformVersion')
				lastSync = util.return_datetime(i.get('lastSync'))
				status = i.get('status')
				print serialNumber
				csvwriter.writerow([serialNumber, deviceId, macAddress, 
					annotatedAssetId, annotatedLocation, annotatedUser, notes, 
					orgUnitPath, model, platformVersion, lastSync, status])


def get_chromedevices():
	chrome_devices = gservice.directoryservice.paginate(cd
				, method='chromeosdevices'
				, customerId='my_customer')
	return chrome_devices


def dump_chromebooks(json_file_path):
	with open(json_file_path, 'wb') as f:
		f.write(json.dumps(chrome_devices, indent=4))


def json_dump_csv(csv_file_path, json):
	with open(csv_file_path, 'wb') as f:
		csvwriter = csv.writer(f, delimiter=',', quotechar="\"", 
								quoting=csv.QUOTE_MINIMAL)
		csvwriter.writerow(['serialNumber', 'deviceId', 'macAddress',
			'annotatedAssetId', 'annotatedLocation', 'annotatedUser',
			'notes', 'orgUnitPath', 'model', 'platformVersion', 'lastSync'])
		for i in json:
			serialNumber = i.get('serialNumber')
			deviceId = i.get('deviceId')
			macAddress = i.get('macAddress')
			annotatedAssetId = i.get('annotatedAssetId')
			annotatedLocation = i.get('annotatedLocation')
			annotatedUser = i.get('annotatedUser')
			orgUnitPath = i.get('orgUnitPath')
			notes = i.get('notes')
			model = i.get('model')
			platformVersion = i.get('platformVersion')
			lastSync = i.get('lastSync')
			model = i.get('model')

			csvwriter.writerow([serialNumber, deviceId, macAddress, 
				annotatedAssetId, annotatedLocation, annotatedUser, notes, 
				orgUnitPath, model, platformVersion, lastSync, model])
	

def read_csv_updates(csv_file_path):
	with open(csv_file_path, 'rbu') as f:
		csvreader = csv.reader(f, delimiter=',', quotechar='\"')
		updates = []
		headers = csvreader.next()
		# serial_idx = headers.index('serialNumber') # Checks the index of serialNumber
		for i in csvreader:
			updates.append(dict(zip(headers, i)))
		return updates


def update_device(deviceid=None, user=None, assetid=None, location=None, notes=None, orgunitpath=None):
	"""
	user - the user account associated with the enrolled device
	assetid - the asset tracking number assigned to the device
	location - the primary location of the device or name of the chromecart
	orgunitpath - the full path of the organization unit where the device is 
		enrolled
	"""
	patchbody = {}

	if user:
		patchbody['annotatedUser'] = user

	if assetid:
		patchbody['annotatedAssetId'] = assetid

	if location:
		patchbody['annotatedLocation'] = location

	if orgunitpath:
		patchbody['orgUnitPath'] = orgunitpath

	if notes:
		patchbody['notes'] = notes

	if patchbody:
		response = cd.patch(customerId='my_customer', deviceId=deviceid, body=patchbody).execute()
		print 'Patched'
		return response



dv = Devices()

def update_devices(csv_file_path):
	updating = 0
	updates = read_csv_updates(csv_file_path)
	for device in updates:
		deviceid = dv._get_deviceid(device['serialNumber'])
		user = device['annotatedUser']
		assetid = device['annotatedAssetId']
		location = device['annotatedLocation']
		orgunitpath = device['orgUnitPath']
		notes = device['notes']
		update = dv.update_device(device['serialNumber'], user=device['annotatedUser'], assetid=device['annotatedAssetId'], 
			location=device['annotatedLocation'], notes=device['notes'], orgunitpath=device['orgUnitPath'])
		#print update_device(deviceid=deviceid, orgunitpath=device['orgUnitPath'])
		updating += 1
		print "Updated %d %s %s %s %s %s %s" %(updating, device['serialNumber'], user, assetid, location, orgunitpath, notes)
		print update
		#time.sleep(1)



