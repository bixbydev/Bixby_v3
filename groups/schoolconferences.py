#!/usr/bin/env python

# Filename: schoolconference.py

#=====================================================================#
# Copyright (c) 2015 Bradley Hilton <bradleyhilton@bradleyhilton.com> #
# Distributed under the terms of the GNU GENERAL PUBLIC LICENSE V3.   #
#=====================================================================#

# import time

# import util
# import database.mysql.base
# import database.oracle.base
# from gservice.groups import BatchGroups, BatchMembers

import gservice.groups


new_group_members = """SELECT g.GOOGLE_GROUPID 
								, bu.GOOGLE_ID AS GOOGLE_USERID
		                    	, 'MEMBER' AS role
		                    	, g.GROUP_EMAIL
							FROM bixby_user AS bu
							JOIN staff_py AS sp
								ON bu.EXTERNAL_UID = sp.EXTERNAL_UID
									AND bu.USER_TYPE = 'Staff'
							JOIN groups AS g
								ON sp.SCHOOLID = g.DEPARTMENT_ID
									AND g.GROUP_TYPE = 'SchoolConference'
							LEFT OUTER JOIN group_member AS gm
								ON bu.GOOGLE_ID = gm.GOOGLE_USERID
									AND g.GOOGLE_GROUPID = gm.GOOGLE_GROUPID
							WHERE bu.SUSPENDED = 0
							AND sp.EXTERNAL_USERSTATUS = 1 -- True/Active False/Inactive
							AND gm.GOOGLE_GROUPID IS NULL
							AND bu.GOOGLE_ID IS NOT NULL
							-- LIMIT 1
							"""


def main():
	#insert_new_group_members(new_group_members)
	gservice.groups.insert_new_group_members(new_group_members)
	

if __name__ == '__main__':
	main()


