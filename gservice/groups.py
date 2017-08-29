#!/usr/bin/env python

# Filename: groups.py

#=====================================================================#
# Copyright (c) 2015 Bradley Hilton <bradleyhilton@bradleyhilton.com> #
# Distributed under the terms of the GNU GENERAL PUBLIC LICENSE V3.   #
#=====================================================================#

import json
import time

from googleapiclient.model import makepatch
from googleapiclient.http import BatchHttpRequest

from config import config
import database.mysql.base
from database.mysql.base import CursorWrapper
from database import queries
from logger.log import log
from gservice.directoryservice import DirectoryService, paginate
import util


group_schema = """{
    "title": "Bixby Group Schema",
    "type": "object",
    "schemaVersion": "v1",
    "dbTable": "groups",
    "properties": {
        "name": {
            "type": "string",
            "required": true,
            "column": "GROUP_NAME",
            "api": true
        },
        "adminCreated": {
            "description": "Group was created by an Admin User. Read-Only",
            "type": "boolean",
            "readonly": true,
            "api": true
        },
        "directMembersCount": {
            "description": "Group direct members count",
            "type": "integer",
            "readonly": true,
            "api": false
        },
        "email": {
            "type": "string",
            "required": true,
            "column": "group_email",
            "api": true
        },
        "id": {
            "type": "string",
            "required": true,
            "column": "google_groupid",
            "api": true
        },
        "description": {
            "type": "string",
            "column": "description",
            "api": true
        },
        "etag": {
            "type": "string",
            "column": "etag",
            "description": "The etag of the google groups resource",
            "api": true
        },
        "department_id":{
            "type": "integer",
            "required": false,
            "column": "department_id",
            "api": false
        },
        "group_type":{
            "type": "string",
            "required": false,
            "column": "group_type",
            "api": false
        },
        "unique_attribute":{
            "type": "string",
            "required": false,
            "column": "unique_attribute",
            "api": false
        }
    }
}"""


member_schema = """{
    "title": "Bixby Member Schema",
    "type": "object",
    "schemaVersion": "v1",
    "dbTable": "group_member",
    "properties": {
        "role": {
            "type": "string",
            "required": true,
            "column": "role",
            "enum": ["MEMBER", "OWNER", "MANAGER"],
            "api": true
        },
        "type": {
            "type": "string",
            "required": false,
            "column": "type",
            "enum": ["USER","GROUP"],
            "api": true
        },
        "email": {
            "type": "string",
            "required": false,
            "api": true
        },
        "id": {
            "description": "The google ID of the member",
            "type": "string",
            "required": true,
            "column": "google_userid",
            "api": true
        },
        "etag": {
            "type": "string",
            "column": "etag",
            "description": "The etag of the google groups resource",
            "api": true
        },
        "googleGroupID": {
            "description": "This would be the ID not email of the group",
            "type": "string",
            "column": "GOOGLE_GROUPID",
            "api": false
        }
    }
}"""


class SchemaBuilder(object):
    __DATA_TYPES__ = {
        "string": basestring,
        "boolean": bool,
        "integer": int,
        "object": dict
    }

    schema_props = {}

    def __init__(self, schema):
        """Takes a schema object as a valid JSON string"""
        self.schema = json.loads(schema)
        self.schema_props = self.schema.get('properties')
        self.db_table = self.schema.get('dbTable')
        # self.api_payload = {}
        #self.db_payload = {}
        for prop in self.schema_props:
            self.__setattr__(prop, None)

    def __setattr__(self, name, value):
        if name in self.schema_props.iterkeys():
            pass
            #print name, value #Remove
            # if self.__validate_property(name, value):
            #     if self.schema_props.get(name).get('api', False):
            #         self.api_payload[name] = value
        object.__setattr__(self, name, value)

    def __is_valid_data_type(self, data_type, value):
        """This is the messy part, but it works for now"""

        data_type = self.__DATA_TYPES__[data_type]
        if isinstance(value, data_type):
            #print 'valid value' #Remove
            return True
        elif value == None:
            #print "None value: ", data_type, value #Remove
            return False
        elif data_type == int:
            value = valid_int(value)
            #print "integer value %d" %value
        else:
            raise TypeError(
                'Invalid Value: {0} Data Type {1}'.format(value, data_type) )

    def __validate_property(self, prop_name, value):
        data_type = self.schema_props.get(prop_name).get('type')
        is_valid = self.__is_valid_data_type(data_type, value)
        # print is_valid
        #description = prop_name.get('description')
        required = self.schema_props.get(prop_name).get('required', False)
        if is_valid == False or required and value is None:
            return False
        else:
            return True

    def db_cols(self):
        dbcols = {}
        for key in self.__dict__.iterkeys():
            prop = self.schema_props.get(key)
            if prop:
                col = prop.get('column', None)
                if col and self.__dict__.get(key):
                    dbcols[col] = self.__dict__.get(key)
        return dbcols

    def api_fields(self):
        payload = {}
        for key in self.__dict__.iterkeys():
            prop = self.schema_props.get(key)
            if prop and prop.get('api', False):
                value = self.__dict__.get(key)
                if self.__dict__.get(key):
                    # and self.__validate_property(name, value): VALIDATE!!! BEH
                    payload[key] = value
        return payload

    @property
    def db_payload(self):
        return self.db_cols()

    @property
    def api_payload(self):
        return self.api_fields()

    def __clear_schema_props(self):
        for prop in self.schema_props:
            self.__setattr__(prop, None)

    def construct(self, **kwargs):
        self.__clear_schema_props()
        for arg, value in kwargs.iteritems():
            self.__setattr__(arg, value)


class BatchBase(BatchHttpRequest, DirectoryService, SchemaBuilder, CursorWrapper):
    """Base class for Batching group and group member info.
    This class will eventually be extendable to other services"""
    def __init__(self, schema):
        self.__schema = schema
        DirectoryService.__init__(self)
        SchemaBuilder.__init__(self, self.__schema)
        CursorWrapper.__init__(self)
        self.__init_batch()

    def __init_batch(self):
        self.batch = BatchHttpRequest()
        self.request_id = 1
        self.requests = {}

    def __increment_request_id(self):
        self.request_id += 1

    def _add_request(self, request, request_id):
        """Add the request to the requests dictionary with an ID that 
        can be used to lookup details about the request that aren't in the 
        response from Google."""
        if self.requests.get(str(request_id)) == None:
            self.requests[str(self.request_id)] = request
        else:
            raise KeyError("Used Request_ID")

        if self.request_id == request_id:
            self.__increment_request_id()

    def return_request_body(self, request_id, response, exception):
        print '#' * 10
        print json.dumps(response, indent=4)
        #self.construct(**response)
        #print self.db_payload
        print '#' * 5
        print self.requests.get(request_id)

    def execute(self):
        #print self.requests
        self.batch.execute(http=self.http)
        self.__init_batch()


class BatchMembers(BatchBase):
    def __init__(self):
        BatchBase.__init__(self, member_schema)
        self.service = self.members()

    def insert_member(self, group_key, member_key, member_type, request_id=None):
        if request_id == None:
            request_id = self.request_id
        request = {'groupKey': group_key
                    , 'request_type': 'insert_member'
                    , 'memberKey': member_key}
        self._add_request(request, request_id=request_id)
        request_body = { 'id': member_key, 'role': member_type }
        service_call = self.service.insert(groupKey=group_key, body=request_body)
        self.batch.add(service_call, callback=self._insert_member_in_db,
                        request_id=str(request_id)) 

    def _insert_member_in_db(self, request_id, response, exception):
        request = self.requests.get(request_id)
        group_key = request.get('groupKey')
        member_key = request.get('memberKey')

        if exception is not None:
            if exception.resp.status == 409: #Member Exists
                time.sleep(0.2)
                response = self.service.get(groupKey=group_key, 
                                            memberKey=member_key).execute()
            log.error(exception)

        if response:
            self.construct(**response)
        #from the global function below
        log.info('Added Member to Group: %s' %self.db_payload)
        insert_group_member(self.cursor, self.db_table, group_key, self.db_payload)

    def delete_member(self, group_key, member_key, request_id=None):
        if request_id == None:
            request_id = self.request_id
        request = {'groupKey': group_key
                    , 'request_type': 'delete_member'
                    , 'memberKey': member_key}
        self._add_request(request, request_id=request_id)
        service_call = self.service.delete(groupKey=group_key,
                                            memberKey=member_key)
        self.batch.add(service_call, callback=self._delete_group_member, 
                        request_id=str(request_id))

    def _delete_group_member(self, request_id, response, exception):
        request = self.requests.get(request_id)
        group_key = request.get('groupKey')
        member_key = request.get('memberKey')
        sql = """DELETE FROM group_member 
                    WHERE google_groupid = %s
                    AND google_userid = %s"""
        if exception is not None:
            log.error(exception)
            time.sleep(2)
        else:
            logstring = "Removed Member: {0} from Group: {1}"
            log.info(logstring.format(member_key, group_key))
            self.cursor.execute(sql, (group_key, member_key))


class BatchGroups(BatchBase):
    def __init__(self):
        BatchBase.__init__(self, group_schema)
        self.service = self.groups()

    def get_group(self, group_key, request_id=None):
        if request_id == None:
            request_id = self.request_id
        request = { 'groupKey': group_key, 'request_type':'get'  }
        self._add_request(request, request_id=request_id)
        self.batch.add(self.service.get(groupKey=group_key),
                    # callback=self.return_request_body,
                    callback=self._get_group_info_from_db,
                    request_id=str(request_id))

    def delete_group(self, group_key, request_id=None):
        if request_id == None:
            request_id = self.request_id
        request = { 'groupKey' : group_key, 'request_type' : 'delete' }
        self._add_request(request, request_id)
        self.batch.add(self.service.delete(groupKey=group_key),
                        callback=self._delete_group_from_db,
                        request_id=str(request_id))

    def insert_group(self, email=None, name=None, description=None, 
                    unique_attribute=None, department_id=None,
                    group_type=None):
        request_id = self.request_id
        group_object = {}
        print email
        if email is None:
            raise ValueError('email cannot be None')
        else:
            group_object['email'] = email

        if name:
            group_object['name'] = name

        if description:
            group_object['description'] = description

        if unique_attribute:
            group_object['unique_attribute'] = unique_attribute

        if department_id:
            group_object['department_id'] = department_id

        if group_type:
            group_object['group_type'] = group_type

        self.construct(**group_object)
        #db_payload = self.db_payload 
        print 'Constructed: ', self.db_payload
        request = {'db_payload' : self.db_payload, 
                   'request_type' : 'insert' }
        self._add_request(request, request_id)
        self.batch.add(self.service.insert(body=self.api_payload),
                        callback=self._insert_group_in_db,
                        request_id=str(request_id))


    def _insert_group_in_db(self, request_id, response, exception):
        print request_id
        request = self.requests.get(request_id)
        print '--------------------'
        print request 
        db_payload = request.get('db_payload')
        print '################# %s ##################' %db_payload.get('group_email')
        payload = {}

        if exception is not None:
            if exception.resp.status == 409: #Entity Exists
                log.error('Could not create group. Entity Exists %s' %db_payload)
                log.warn('This is normal. Likely two classes this period')
                time.sleep(0.5)
                existing_group = self.service.get(groupKey=db_payload.get('group_email')).execute()
                payload.update(db_payload)
                payload.update(existing_group)
                self.construct(**payload)
                util.insert_json_payload('groups', self.db_payload, self.cursor)
            log.critical(exception)

        elif response is None:
            log.critical('No response from google: %s' %db_payload)
            
        else:
            # The orginal db_payload is combined with the response for storage in db
            payload.update(db_payload)
            payload.update(response)
            print payload
            self.construct(**payload)
            print self.db_payload
            util.insert_json_payload('groups', self.db_payload, self.cursor)


    def _delete_group_from_db(self, request_id, response, exception):
        if exception is not None:
            log.error(exception)
        else:
            request = self.requests.get(request_id)
            group_key = request.get('groupKey')
            sql = """DELETE FROM groups WHERE google_groupid = %s"""
            if group_key:
                self.cursor.execute(sql, (group_key,))
            #print str(request)
            log.warn('Permanantly Deleted Group: {0}'.format(request))


    def _get_group_info_from_db(self, request_id, response, exception):
        request = self.requests.get(request_id)
        group_key = request.get('groupKey')
        sql = """SELECT * FROM groups WHERE google_groupid = %s"""
        if group_key:
            self.cursor.execute(sql, (group_key,))
            print self.cursor.fetchall()


class BatchGroupMembers(BatchHttpRequest, DirectoryService, SchemaBuilder):
    def __init__(self):
        self.batch = BatchHttpRequest()
        DirectoryService.__init__(self)
        SchemaBuilder.__init__(self, member_schema)
        self.ms = self.members()
        self.request_id = 5
        self.requests = {}

    def __increment_request_id(self):
        self.request_id += 1

    def _add_request(self, request, request_id):
        if self.requests.get(str(request_id)) == None:
            self.requests[str(self.request_id)] = request
        else:
            raise KeyError("Used Request_ID")

        if self.request_id == request_id:
            self.__increment_request_id()

    def get_member(self, group_key, member_key, request_id=None):
        if request_id == None:
            request_id = self.request_id
        request = (group_key, member_key, 'get')
        self._add_request(request, request_id=request_id)
        self.batch.add(self.ms.get(groupKey=group_key, memberKey=member_key)
                    , callback=self._update_member, request_id=str(request_id))

    def delete_member(self, group_key, member_key, request_id=None):
        if request_id == None:
            request_id = self.request_id
        self._add_request(request)

    def print_member(self, request_id, response, exception):
        print request_id, response, exception
        print self.requests.get(request_id)

    def _update_member(self, request_id, response, exception):
        self.construct(**response)
        print self.db_payload
        print self.requests.get(request_id)

    def patch_member(self, group_key, member_key, body):
        pass

    def execute(self):
        self.batch.execute(http=self.http)


def valid_int(value):
    if isinstance(value, int):
        return value
    else:
        return int(value)


def populate_group_id(json_file_path):
    """This is supposed to be a one time only use to load the GOOGLE_GROUPID"""
    mcon = database.mysql.base.CursorWrapper()
    mc = mcon.cursor
    gs = SchemaBuilder(member_schema)

    with open(json_file_path, 'rU') as f:
        groups = json.loads(f.read())
    for group in groups:
        gs.new_group(**group)
        update_from_dictionary(mc, 'groups', gs.email, gs.db_payload)


def update_from_dictionary(cursor, table, unique_id, db_dict):
    """A database cursor
        The table name
        The unique id used to identify the row/s in the table 
            # TODO (bixbydev): Explain that above one more clearly
        The db_dict should be a dictionary with column names as keys and
            values as the row's column value"""
    cursor.execute('SELECT GROUP_EMAIL FROM %s WHERE GROUP_EMAIL = %s', (table, unique_id))
    if cursor.fetchone():
        update = 'UPDATE {} SET {}'
        columns = update.format(table, ', '.join('{}=%s'.format(k) for k in db_dict))
        where = """WHERE GROUP_EMAIL = \'%s\'""" %unique_id
        sql = columns + where
        log.info('Updated Group: %s' %str(db_dict.items()) )
        # print sql, db_dict.values()

    else:
        places = ', '.join(['%s'] * len(db_dict))
        columns = ', '.join(db_dict.keys())
        sql = """INSERT INTO %s (%s) VALUES (%s)""" %(table, columns, places)
        log.info('Inserted Unmanaged Group: %s' %str(db_dict.items()) )

    log.info(sql)
    cursor.execute(sql, db_dict.values())


def populate_group_members(cursor, member_service, google_groupid):
    gm = SchemaBuilder(member_schema)
    try:
        members = paginate(member_service, method='members', groupKey=google_groupid)
    except KeyError:
        members = []
        log.warn('No Group Members for GroupID: %s' %google_groupid)
    for member in members:
        gm.new_group(**member)
        insert_member(cursor, gm.db_table, google_groupid, gm.db_payload)


def insert_new_groups(new_groups_query):
    """Takes an SQL Query with (group email, group name, description, unique_attribute
            department_id, group_type)"""
    bg = BatchGroups()
    bg.cursor.execute(new_groups_query)
    new_groups = bg.cursor.fetchall()
    fields = [i[0] for i in bg.cursor.description]
    chunked_groups = util.list_chunks(list(new_groups), 20)
    for chunk in chunked_groups:
        for group in chunk:
            bg.insert_group(email=group[0], 
                            name=group[1], 
                            description=group[2], 
                            unique_attribute=group[3], 
                            department_id=group[4],
                            group_type=group[5] )
        time.sleep(3)
        bg.execute()


def insert_new_group_members(members_query):
    """Takes an SQL Query with (groupKey, memberKey, role)"""
    bg = BatchMembers()
    bg.cursor.execute(members_query)
    new_members = bg.cursor.fetchall()
    fields = [i[0] for i in bg.cursor.description]
    chunked_members = util.list_chunks(list(new_members), 20)
    for chunk in chunked_members:
        for mem in chunk:
            print mem
            bg.insert_member(mem[0], mem[1], mem[2])
        time.sleep(2)
        bg.execute()


def delete_group_members(members_query):
    """Takes an SQL Query with (groupKey, memberKey)"""
    bg = BatchMembers()
    bg.cursor.execute(members_query)
    new_members = bg.cursor.fetchall()
    fields = [i[0] for i in bg.cursor.description]
    chunked_members = util.list_chunks(list(new_members), 20)
    for chunk in chunked_members:
        for mem in chunk:
            print mem
            bg.delete_member(mem[0], mem[1])
            time.sleep(0.1)
        time.sleep(1)
        bg.execute()


def insert_group_member(cursor, table, google_groupid, db_payload):
    """This was used for the initial sync of members.
    This funciton is replaced by insert_group_member()
    It is also unknown what happened to insert_group_member()
    This will now become insert_group_member()
    """
    places = '%s, '+', '.join(['%s'] * len(db_payload))
    db_keys = db_payload.keys()
    db_keys.insert(0, 'GOOGLE_GROUPID')
    columns = ', '.join(db_keys)
    values = db_payload.values()
    values.insert(0, google_groupid)
    update_cols = ', '.join([i+"=%s" for i in db_keys])
    #print update_cols %values
    sql = """INSERT INTO %s (%s) VALUES (%s)
                ON DUPLICATE KEY 
                    UPDATE %s\n""" %(table, columns, places, update_cols)
    values += values
    #log.debug((sql) %tuple(values))
    cursor.execute(sql, values)

 
def refresh_all_group_members(overwrite=False):
    mcon = database.mysql.base.CursorWrapper()
    mc = mcon.cursor
    ds = DirectoryService()
    ms = ds.members()
    sql = """SELECT GOOGLE_GROUPID, GROUP_EMAIL 
            FROM groups
            -- ORDER BY RAND()
            -- LIMIT 2
        """
    mc.execute(sql)
    all_groups = mc.fetchall()
    for group in all_groups:
        log.info("""Refreshing Group: %s""" %group[1])
        if overwrite:
            delete = """DELETE FROM group_member
                            WHERE GOOGLE_GROUPID = %s"""
            mc.execute(delete, group[0])

        populate_group_members(mc, ms, group[0])


### Was all the above a waste of time? ###

def batch_delete_groups(list_of_groupids):
    """Takes a list of group keys, they must be the Google Unique Group ID"""
    bg = BatchGroups()
    chunked_groups = util.list_chunks(list_of_groupids, 200)
    for groups in chunked_groups:
        time.sleep(5)
        print 'Chunked'
        for groupid in groups:
            # bg.get_group(groupid)
            bg.delete_group(groupid)
        log.info('Batch Deleting {0} Groups'.format(len(bg.requests)))
        time.sleep(1)
        bg.execute() # Delete the Extra Groups


def pull_groups():
    m = CursorWrapper()
    m.cursor.execute("""SELECT GOOGLE_GROUPID
                        FROM groups g
                        WHERE g.GROUP_TYPE = 'StudentSection'""")

    groups = m.cursor.fetchall()
    return [i[0] for i in groups]


def test_create_group(email=None, name=None, description=None, 
    unique_attribute=None, department_id=None, group_type=None, **kwargs):
    gs = SchemaBuilder(group_schema)
    group_object = {}
    if email is None:
        raise ValueError('email cannot be None')
    else:
        group_object['email'] = email

    if name:
        group_object['name'] = name

    if description:
        group_object['description'] = description

    if unique_attribute:
        group_object['unique_attribute'] = unique_attribute

    if department_id:
        group_object['department_id'] = department_id

    if group_type:
        group_object['group_type'] = group_type

    gs.construct(**group_object)

    db_payload = gs.db_payload

    print util.insert_json_payload('groups', gs.db_payload)



def main():
    pass


    

if __name__ == '__main__':
    main()

