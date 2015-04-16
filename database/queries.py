#!/usr/bin/env python

# Filename: queries.py

#=====================================================================#
# Copyright (c) 2015 Bradley Hilton <bradleyhilton@bradleyhilton.com> #
# Distributed under the terms of the GNU GENERAL PUBLIC LICENSE V3.   #
#=====================================================================#



get_staff_from_sis = """SELECT t.id
					, t.schoolid
					, t.teachernumber
					, CASE WHEN ps_customfields.getcf(\'teachers\',t.id,\'CA_SEID\') IS NOT NULL THEN 1 ELSE 0 END CERTIFICATED
					, t.first_name
					, t.last_name
					, t.middle_name
					, ps_customfields.getcf(\'teachers\',t.id,\'gender\') gender
					, CASE WHEN t.status = 1 THEN 0 ELSE 2 END EXTERNAL_USERSTATUS
					, t.staffstatus STAFF_TYPE
					, CASE WHEN ps_customfields.getcf(\'teachers\',t.id,\'BUSD_NoEmail\')=1 THEN 1
						ELSE 0 END SUSPEND_ACCOUNT
					, CASE WHEN ps_customfields.getcf(\'teachers\',t.id,\'BUSD_Email\')=1 THEN 1
						ELSE 0 END BUSD_Email
					, ps_customfields.getcf(\'teachers\',t.id,\'BUSD_Email_Address\')
					, CASE WHEN ps_customfields.getcf(\'teachers\',t.id,\'CA_SEID\') IS NOT NULL THEN 1
  						ELSE 2 END STAFF_CONFERENCE 
					FROM teachers t
					WHERE t.staffstatus IS NOT NULL
					AND t.schoolid = t.homeschoolid
					AND t.first_name IS NOT NULL -- Remove for Production Version Fix
					AND t.last_name IS NOT NULL  -- Remove for Production Version Fix
					"""

get_students_from_sis = """SELECT id STUDENTID
						, SCHOOLID
						, STUDENT_NUMBER
						, FIRST_NAME
						, LAST_NAME
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
insert_staff_py = """INSERT INTO STAFF_PY (STAFFID
						, SCHOOLID
						, TEACHERNUMBER
						, CERTIFICATED
						, FIRST_NAME
						, LAST_NAME
						, MIDDLE_NAME
						, GENDER
						, EXTERNAL_USERSTATUS
						, STAFF_TYPE						
						, SUSPEND_ACCOUNT
						, BUSD_Email
						, BUSD_Email_Address
						, Staff_Conference)
						VALUES(%s, %s, %s, %s, %s
							, %s, %s, %s, %s, %s
							, %s, %s, %s, %s)"""




insert_students_py = """INSERT INTO STUDENTS_PY (STUDENTID
											, SCHOOLID
											, STUDENT_NUMBER
											, FIRST_NAME
											, LAST_NAME
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


