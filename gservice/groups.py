#!/usr/bin/env python

# Filename: groups.py

#=====================================================================#
# Copyright (c) 2015 Bradley Hilton <bradleyhilton@bradleyhilton.com> #
# Distributed under the terms of the GNU GENERAL PUBLIC LICENSE V3.   #
#=====================================================================#

import json
import time

from googleapiclient.model import makepatch
from config import config
import database.mysql.base
from database import queries
from logger.log import log
from gservice.directoryservice import DirectoryService, paginate
from util import un_map, return_datetime, json_date_serial


# class ValidTypes(object):
#     valid_types = valid_types
#     def __init__(self):
#         pass

#     def _validate_type(self, value, data_type):
#         return isinstance(value, self.valid_types.get(data_type))

#     @classmethod
#     def validate(cls, value, data_type):

#         return cls._validate(value, data_type)
# https://www.googleapis.com/discovery/v1/apis/directory/v1/rest


group_schema = """{
    "title": "Bixby Group Schema",
    "type": "object",
    "schemaVersion": "v1",
    "dbTable": "groups",
    "properties": {
        "name": {
            "type": "string",
            "required": true,
            "column": "GROUP_NAME"
        },
        "adminCreated": {
            "description": "Group was created by an Admin User. Read-Only",
            "type": "boolean",
            "readonly": true
        },
        "directMembersCount": {
            "description": "Group direct members count",
            "type": "integer",
            "readonly": true
        },
        "email": {
            "type": "string",
            "required": true,
            "column": "GROUP_EMAIL"
        },
        "id": {
            "type": "string",
            "required": true,
            "column": "GOOGLE_ID"
        },
        "description": {
            "type": "string",
            "column": "DESCRIPTION"
        },
        "etag": {
            "type": "string",
            "column": "ETAG",
            "description": "The etag of the google groups resource"
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
            "enum": ["MEMBER", "OWNER", "MANAGER"]
        },
        "type": {
            "type": "string",
            "required": false,
            "column": "TYPE",
            "enum": ["USER","GROUP"]
        },
        "email": {
            "type": "string",
            "required": false
        },
        "id": {
            "description": "The google ID of the member",
            "type": "string",
            "required": true,
            "column": "GOOGLE_USERID"
        },
        "etag": {
            "type": "string",
            "column": "ETAG",
            "description": "The etag of the google groups resource"
        },
        "googleGroupID": {
            "description": "This would be the ID not email of the group",
            "type": "string",
            "column": "GOOGLE_GROUPID"
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
        self.api_payload = {}
        self.db_payload = {}
        for prop in self.schema_props:
            self.__setattr__(prop, None)

    def __setattr__(self, name, value):
        if name in self.schema_props.iterkeys():
            #print name, value #Remove
            if self.__validate_property(name, value):
                self.api_payload[name] = value
                db_col = self.schema_props.get(name).get('column', None)
                if db_col:
                    self.db_payload[db_col] = value
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

    def new_group(self, **kwargs):
        for arg, value in kwargs.iteritems():
            #print arg, value #Remove
            self.__setattr__(arg, value)





def valid_int(value):
    if isinstance(value, int):
        return value
    else:
        return int(value)


def populate_group_id(json_file_path):
    """This is supposed to be a one time only use to load the GOOGLE_ID"""
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
    cursor.execute(sql, db_dict.values() )


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
    sql = """SELECT GOOGLE_ID, GROUP_EMAIL 
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
                            WHERE GOOGLE_ID = %s"""
            mc.execute(delete, group[0])

        populate_group_members(mc, ms, group[0])




def main():
    pass

    

if __name__ == '__main__':
    main()




