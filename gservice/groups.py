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
from database import queries
from logger.log import log
from gservice.directoryservice import DirectoryService, paginate
from util import un_map, return_datetime, json_date_serial

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
            "column": "GROUP_EMAIL",
            "api": true
        },
        "id": {
            "type": "string",
            "required": true,
            "column": "GOOGLE_GROUPID",
            "api": true
        },
        "description": {
            "type": "string",
            "column": "DESCRIPTION",
            "api": true
        },
        "etag": {
            "type": "string",
            "column": "ETAG",
            "description": "The etag of the google groups resource",
            "api": true
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
            "column": "ROLE",
            "enum": ["MEMBER", "OWNER", "MANAGER"],
            "api": true
        },
        "type": {
            "type": "string",
            "required": false,
            "column": "TYPE",
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
            "column": "GOOGLE_USERID",
            "api": true
        },
        "etag": {
            "type": "string",
            "column": "ETAG",
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
            #print arg, value #Remove
            self.__setattr__(arg, value)





class ManageGroups(SchemaBuilder, DirectoryService):
    def __init__(self, schema=member_schema):
        SchemaBuilder.__init__(self, schema)
        DirectoryService.__init__(self)
        self.gs = self.groups()
        self.ms = self.members()


class ManageMembers(DirectoryService):
    def __init__(self):
        DirectoryService.__init__(self)
        self.ms = self.members()

    def add_member(self, group_key, body):
        pass






class BatchGroupMembers(BatchHttpRequest, DirectoryService):
    def __init__(self):
        self.batch = BatchHttpRequest()
        DirectoryService.__init__(self)
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
        self.batch.add(self.ms.get(groupKey=group_key, memberKey=member_key), callback=self.print_member, request_id=str(request_id))

    def print_member(self, request_id, response, exception):
        print request_id, response, exception
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


def insert_json_payload(cursor, table, payload):
    places = ', '.join(['%s'] * len(payload))
    col_names = payload.keys()
    columns = ', '.join(col_names)
    values = payload.values()
    update_cols = ', '.join([i+"=%s" for i in col_names])
    sql = """INSERT INTO %s (%s) VALUES (%s)
                ON DUPLICATE KEY 
                    UPDATE %s\n""" %(table, columns, places, update_cols)
    log.info('Inserted User to Group: %s' %str(values))
    values += values
    #print sql %tuple(values)
    log.debug((sql) %tuple(values))
    cursor.execute(sql, values)


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


def insert_member(cursor, table, google_groupid, db_payload):
    """This was used for the initial sync of members.
    This funciton is replaced by insert_group_member()"""
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
    log.debug('Inserted User Into Group: %s' %str(values))
    values += values
    # log.debug((sql) %tuple(values))
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

def insert_or_update_group():
    pass









def main():
    pass

    

if __name__ == '__main__':
    main()




