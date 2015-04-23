#!/usr/bin/env python

# Filename: queries.py

#=====================================================================#
# Copyright (c) 2015 Bradley Hilton <bradleyhilton@bradleyhilton.com> #
# Distributed under the terms of the GNU GENERAL PUBLIC LICENSE V3.   #
#=====================================================================#


get_staff_from_sis = """SELECT t.USERS_DCID EXTERNAL_UID
, t.id
, t.schoolid
, t.teachernumber
, CASE WHEN ps_customfields.getcf('teachers',t.id,'CA_SEID') IS NOT NULL THEN 1 ELSE 0 END CERTIFICATED
, t.first_name given_name
, t.last_name family_name
, t.middle_name
, ps_customfields.getcf('teachers',t.id,'gender') gender
, CASE WHEN t.status = 1 THEN 0 ELSE 2 END EXTERNAL_USERSTATUS
, t.staffstatus STAFF_TYPE
, CASE WHEN ps_customfields.getcf('teachers',t.id,'BUSD_NoEmail')=1 THEN 1
	ELSE 0 END SUSPEND_ACCOUNT
, CASE WHEN ps_customfields.getcf('teachers',t.id,'BUSD_Email')=1 THEN 1
	ELSE 0 END BUSD_Email
, ps_customfields.getcf('teachers',t.id,'BUSD_Email_Address')
, CASE WHEN ps_customfields.getcf('teachers',t.id,'CA_SEID') IS NOT NULL THEN 1
	ELSE 2 END STAFF_CONFERENCE 
FROM teachers t
WHERE t.staffstatus IS NOT NULL
	AND t.schoolid = t.homeschoolid
	AND t.first_name IS NOT NULL -- Remove for Production Version Fix
	AND t.last_name IS NOT NULL  -- Remove for Production Version Fix
	"""


get_students_from_sis = """SELECT id EXTERNAL_UID
						, SCHOOLID
						, STUDENT_NUMBER
						, FIRST_NAME GIVEN_NAME
						, LAST_NAME FAMILY_NAME
						, MIDDLE_NAME
						, DOB
						, CASE WHEN UPPER(gender) NOT IN ('M','F') THEN 'U' ELSE UPPER(gender) END GENDER
						, GRADE_LEVEL
						, HOME_ROOM
						, ps_customfields.getcf('students',id,'Area') AREA
						, TO_DATE(entrydate) ENTRYDATE
						, TO_DATE(exitdate) EXITDATE
						, CASE WHEN enroll_status = 0 THEN 0 ELSE 2 END EXTERNAL_USERSTATUS
						, CASE WHEN ps_customfields.getcf('students',id,'BUSD_Gmail_Suspended')=1 THEN 1
							ELSE 0 END SUSPEND_ACCOUNT
						      -- The Student Web ID and Parent Web_ID are here for other purposes maybe I will put that into a custom table
						, NULL EMAIL_OVERRIDE -- Pull from field BUSD_EMAIL_OVERRIDE
						, STUDENT_WEB_ID
						, WEB_ID PARENT_WEB_ID
						FROM students
						-- WHERE id BETWEEN 5000 AND 5200 
						-- AND grade_level >= 3
						ORDER BY id
						"""


# MySQL Queries
insert_staff_py = """INSERT INTO STAFF_PY (EXTERNAL_UID
						, STAFFID
						, SCHOOLID
						, TEACHERNUMBER
						, CERTIFICATED
						, GIVEN_NAME
						, FAMILY_NAME
						, MIDDLE_NAME
						, GENDER
						, EXTERNAL_USERSTATUS
						, STAFF_TYPE						
						, SUSPEND_ACCOUNT
						, BUSD_Email
						, BUSD_Email_Address
						, Staff_Conference)
						VALUES(%s, %s, %s, %s, %s, %s
							, %s, %s, %s, %s, %s
							, %s, %s, %s, %s)"""



insert_students_py = """INSERT INTO STUDENTS_PY (EXTERNAL_UID
											, SCHOOLID
											, STUDENT_NUMBER
											, FIRST_NAME
											, FAMILY_NAME
											, MIDDLE_NAME
											, DOB
											, GENDER
											, GRADE_LEVEL
											, HOME_ROOM
											, AREA
											, ENTRYDATE
											, EXITDATE
											, EXTERNAL_USERSTATUS
											, SUSPEND_ACCOUNT
											, EMAIL_OVERRIDE
											, STUDENT_WEB_ID
											, PARENT_WEB_ID
											) 
									VALUES (%s, %s, %s, %s, %s
											, %s, %s, %s, %s, %s
											, %s, %s, %s, %s, %s
											, %s, %s, %s)"""

sql_get_bixby_user = """SELECT ID
						, USER_TYPE
						, PRIMARY_EMAIL
						, GIVEN_NAME
						, FAMILY_NAME
						, EXTERNAL_UID
						, SUSPENDED
						, CHANGE_PASSWORD
						, GLOBAL_ADDRESSBOOK
						, OU_PATH
						
						FROM bixby_user
						WHERE PRIMARY_EMAIL = %s"""

get_bixby_id = """SELECT id 
					FROM bixby_user
					WHERE external_uid = %s
						AND user_type = %s"""

lookup_external_uid = """SELECT NEW_EXTERNAL_UID
						FROM lookup_table
						WHERE PRIMARY_EMAIL = %s"""

get_userinfo_vary_params = """SELECT ID
						, USER_TYPE
						, PRIMARY_EMAIL
						, GIVEN_NAME
						, FAMILY_NAME
						, EXTERNAL_UID
						, SUSPENDED
						, CHANGE_PASSWORD
						, GLOBAL_ADDRESSBOOK
						, OU_PATH
						
						FROM bixby_user
						%s """

