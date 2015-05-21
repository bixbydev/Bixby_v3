#!/usr/bin/env python

# Filename: schoolconference.py

#=====================================================================#
# Copyright (c) 2015 Bradley Hilton <bradleyhilton@bradleyhilton.com> #
# Distributed under the terms of the GNU GENERAL PUBLIC LICENSE V3.   #
#=====================================================================#


from apiclient.errors import HttpError
from gservice import groups
from util import chunks
import database.mysql.base as mysql


new_group_members_sql = """SELECT bu.PRIMARY_EMAIL
					, g.GROUP_EMAIL
					, g.GOOGLE_ID AS GOOGLE_GROUPID
					, bu.GOOGLE_ID GOOGLE_USERID
					FROM bixby_user AS bu
					JOIN staff_py AS sp
						ON bu.EXTERNAL_UID = sp.EXTERNAL_UID
							AND bu.USER_TYPE = 'Staff'
					JOIN groups AS g
						ON sp.SCHOOLID = g.DEPARTMENT_ID
							AND g.GROUP_TYPE = 'SchoolConference'
					LEFT OUTER JOIN group_member AS gm
						ON bu.GOOGLE_ID = gm.GOOGLE_USERID
							AND g.google_id = gm.GOOGLE_GROUPID
					WHERE bu.SUSPENDED = 0
					AND sp.EXTERNAL_USERSTATUS = 0
					AND gm.GOOGLE_GROUPID IS NULL
					LIMIT 25
					"""


gm = groups.SchemaBuilder(groups.member_schema)


con = mysql.CursorWrapper()
cursor = con.cursor
cursor.execute(new_group_members_sql)

ds = groups.DirectoryService()
members_service = ds.members()


def new_users():
	new_group_members = cursor.fetchall()
	members_chunks = chunks(new_group_members, 100)
	for chunk in members_chunks:
		for member in chunk:
			google_userid = member[3]
			google_groupid = member[2]
			email = member[0]
			group_key = google_groupid
			gm.construct(email=member[0]
						, googleGroupID=member[2]
						, id=google_userid
						, type='USER'
						, role='MEMBER'
						)
			try:
				response = members_service.insert(groupKey=group_key, body=gm.api_payload).execute()
				gm.construct(**response)
				#groups.insert_member(cursor, 'group_member', google_groupid, gm.db_payload)
				groups.insert_json_payload(cursor, 'group_member', gm.db_payload)
			except HttpError, e:
				groups.log.warn(e)
				if e.resp.status == 409:
					groups.log.info("""Member exists, that happens: %s GOOGLE_GROUPID: %s"""
							%(email, google_groupid))
					response = members_service.get(groupKey=group_key, memberKey=google_userid).execute()
					gm.construct(**response)
					groups.insert_json_payload(cursor, 'group_member', gm.db_payload)
				else:
					raise e
			groups.log.debug(response)
		

def main():
	new_users()

if __name__ == '__main__':
	main()


