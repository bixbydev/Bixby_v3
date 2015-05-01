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
											, GIVEN_NAME
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

sql_get_staff_py = """SELECT bu.ID
							, 'staff' AS USER_TYPE
							, COALESCE(sp.BUSD_EMAIL_ADDRESS
										, bu.PRIMARY_EMAIL) PRIMARY_EMAIL
							, SP.GIVEN_NAME
							, SP.FAMILY_NAME
							, sp.EXTERNAL_UID
							, CASE WHEN sp.EXTERNAL_USERSTATUS + sp.SUSPEND_ACCOUNT >= 1 THEN 1 ELSE 0 END AS SUSPENDED
							, 0 AS CHANGE_PASSWORD
							, 1 GLOBAL_ADDRESSBOOK
							, '/Staff' AS OU_PATH

							FROM staff_py AS sp
							LEFT OUTER JOIN bixby_user AS bu
								ON sp.EXTERNAL_UID = bu.EXTERNAL_UID
									AND bu.USER_TYPE = 'staff'
							        
							WHERE sp.BUSD_EMAIL = 0 -- 0 = Managed by bixby, for now 
								AND PRIMARY_EMAIL IS NOT NULL
								AND sp.EXTERNAL_UID = %s
								AND bu.USER_TYPE = %s
								"""

sql_get_students_py = """SELECT bu.ID
						, 'student' AS USER_TYPE
						, bu.PRIMARY_EMAIL
						, sp.GIVEN_NAME
						, sp.FAMILY_NAME
						, sp.EXTERNAL_UID
						, sp.SUSPEND_ACCOUNT AS SUSPENDED
						, 0 AS CHANGE_PASSWORD
						, 1 GLOBAL_ADDRESSBOOK
						, CONCAT('/', ou.OU_PATH) AS OU_PATH

						FROM students_py AS sp
						LEFT OUTER JOIN bixby_user AS bu
							ON sp.external_uid = bu.EXTERNAL_UID
								AND bu.USER_TYPE = 'student'
						JOIN orgunit AS ou
							ON sp.SCHOOLID = ou.DEPARTMENT_ID
								AND sp.GRADE_LEVEL = ou.MAP_KEY
						WHERE sp.EXTERNAL_UID = %s
							AND bu.USER_TYPE = %s
						"""

get_new_student_py = """SELECT 'student' AS USER_TYPE
						, sp.EMAIL_OVERRIDE
						, sp.GIVEN_NAME
						, sp.FAMILY_NAME
						, sp.MIDDLE_NAME
						, sp.EXTERNAL_UID
						, sp.SUSPEND_ACCOUNT AS SUSPENDED
						, 0 AS CHANGE_PASSWORD
						, 1 GLOBAL_ADDRESSBOOK
						, CONCAT('/', ou.OU_PATH) AS OU_PATH
						FROM students_py AS sp
						JOIN orgunit AS ou
							ON sp.SCHOOLID = ou.DEPARTMENT_ID
								AND sp.GRADE_LEVEL = ou.MAP_KEY
						WHERE sp.EXTERNAL_UID = %s
						"""

get_student_number = """SELECT STUDENT_NUMBER 
						FROM students_py 
						WHERE EXTERNAL_UID = %s"""

get_new_staff_py = """SELECT 'staff' AS USER_TYPE
						, sp.BUSD_Email_Address EMAIL_OVERRIDE
						, sp.GIVEN_NAME
						, sp.FAMILY_NAME
						, sp.MIDDLE_NAME
						, sp.EXTERNAL_UID
						, sp.SUSPEND_ACCOUNT AS SUSPENDED
						, 0 AS CHANGE_PASSWORD
						, 1 GLOBAL_ADDRESSBOOK
						, '/Staff' AS OU_PATH
						FROM staff_py AS sp
						WHERE sp.EXTERNAL_UID = %s"""


get_user_key = """SELECT coalesce(GOOGLE_ID, PRIMARY_EMAIL) userkey
					FROM bixby_user
					WHERE id = %s"""


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

