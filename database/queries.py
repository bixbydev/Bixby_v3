#!/usr/bin/env python

# Filename: queries.py

#=====================================================================#
# Copyright (c) 2015 Bradley Hilton <bradleyhilton@bradleyhilton.com> #
# Distributed under the terms of the GNU GENERAL PUBLIC LICENSE V3.   #
#=====================================================================#

get_staff_from_sis = """SELECT u.user_id EXTERNAL_UID --Illuminate Assigned
, u.local_user_id STAFFID -- PowerSchool DCID (Probably Users DCID)
--, u.address SCHOOLID
, u.address SCHOOLID -- Fix
, u.local_user_id TEACHERNUMBER
, CASE WHEN u.state_id IS NOT NULL THEN '1' ELSE '0' END Certificated
, u.first_name GIVEN_NAME
, u.last_name FAMILY_NAME
, u.middle_name MIDDLE_NAME
, u.gender
, u.active EXTERNAL_USERSTATUS
, 1 STAFF_TYPE
, CASE WHEN u.email1 = '' THEN '1' 
		WHEN u.email1 IS NULL THEN '1' 
        	ELSE '0' END Suspend_Account
, CASE WHEN u.email1 !='' THEN '1'
		WHEN u.email1 IS NOT NULL THEN '1' 
        	ELSE '0'END BUSD_Email
, u.email1 BUSD_Email_Address
, CASE WHEN u.state_id IS NOT NULL THEN '1' ELSE '2' END Staff_Conference

FROM users u
WHERE u.local_user_id !=''
	AND u.address != ''
"""

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
						VALUES(%s, %s, %s, %s, %s,
						 		%s, %s, %s, %s, %s, 
						 		%s, %s, %s, %s, %s)"""

get_students_from_sis = """
SELECT s.student_id EXTERNAL_UID
, sch.site_id SCHOOLID
, s.local_student_id STUDENT_NUMBER
, s.first_name GIVEN_NAME
, s.last_name FAMILY_NAME
, s.middle_name
, s.birth_date DOB
, CASE WHEN s.gender NOT IN ('M','F') THEN 'U' ELSE s.gender END Gender
, CASE WHEN gl.short_name = 'K' THEN '0' 
    WHEN gl.short_name = 'TK' THEN '-1'
    WHEN gl.short_name = 'PRE' THEN '-2'
    ELSE gl.short_name END::INTEGER Grade_Level
, NULL HOME_ROOM    
, house.house_name Area
, sa.entry_date
, sa.leave_date
-- , CASE WHEN '2017-09-10' BETWEEN sa.entry_date AND sa.leave_date THEN 1 ELSE 0 END EXTERNAL_USERSTATUS
, CASE WHEN current_date BETWEEN sa.entry_date AND sa.leave_date THEN 1 ELSE 0 END EXTERNAL_USERSTATUS
, CASE WHEN cf.custom_field_busd_email_suspend=true THEN 1 ELSE 0 END Email_Suspend
-- , cf.custom_field_busd_email_suspend
, cf.custom_field_busd_override_email EMAIL_OVERRIDE
, s.email STUDENT_WEB_ID
, NULL PARENT_WEB_ID
, 1 STUDENT_ALLOWWEBACCESS
, 1 PARENT_ALLOWWEBACCESS
/*
, CASE WHEN cf.custom_field_busd_email_override = '1' THEN '1' 
		ELSE '0' END Email_Override
, CASE WHEN cf.custom_field_busd_email_suspend = '1' THEN '1'
		ELSE '0' END Email_Suspend
*/
FROM students s
JOIN student_session_aff sa
	ON sa.student_id = s.student_id
        AND sa.is_primary_ada = True
        AND sa.entry_date != sa.leave_date
		AND sa.entry_date = (SELECT MAX(ssa.entry_date)
                         		FROM student_session_aff ssa
                         		WHERE ssa.student_id = sa.student_id
                         			AND ssa.entry_date != ssa.leave_date -- Added this thinking it would help
                       				GROUP BY ssa.student_id)
JOIN sessions ses
	ON ses.session_id = sa.session_id
JOIN sites sch
	ON sch.site_id = ses.site_id
JOIN student_custom_fields_data cf
	ON cf.student_id = s.student_id
-- LEFT OUTER JOIN portal.portal_students ps
	-- ON ps.student_id = s.student_id
JOIN grade_levels gl
	ON sa.grade_level_id = gl.grade_level_id

LEFT OUTER JOIN (SELECT sha.student_id, sha.session_id, h.house_name
					FROM student_house_aff sha
					JOIN houses h
					ON h.house_id = sha.house_id) house
         ON house.student_id = s.student_id
            AND ses.session_id = house.session_id


-- WHERE gl.short_name = 'PRE'
WHERE s.student_id NOT IN (42636, 49374, 41514);
"""

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
											, STUDENT_ALLOWWEBACCESS
											, PARENT_ALLOWWEBACCESS
											) 
									VALUES (%s, %s, %s, %s, %s
											, %s, %s, %s, %s, %s
											, %s, %s, %s, %s, %s
											, %s, %s, %s, %s, %s)"""


new_staff_and_students = """SELECT sp.EXTERNAL_UID 
							, 'student'
							FROM students_py AS sp
							LEFT OUTER JOIN bixby_user AS bu
								ON sp.EXTERNAL_UID = bu.EXTERNAL_UID
									AND bu.USER_TYPE = 'student'
							WHERE sp.EXTERNAL_USERSTATUS = 1
							AND sp.SUSPEND_ACCOUNT = 0
							AND bu.PRIMARY_EMAIL IS NULL
							AND current_date() BETWEEN sp.ENTRYDATE AND sp.EXITDATE
							-- AND '2017-09-02' BETWEEN sp.ENTRYDATE AND sp.EXITDATE
							AND sp.SCHOOLID NOT IN ('20000000', '999999999', '6504', '122804')

								UNION 

							SELECT sp.EXTERNAL_UID
							, 'staff' 
							FROM staff_py AS sp
							LEFT OUTER JOIN bixby_user AS bu
								ON sp.EXTERNAL_UID = bu.EXTERNAL_UID
									AND bu.USER_TYPE = 'staff'
							WHERE sp.EXTERNAL_USERSTATUS = 1 -- Changed Default Value True/Active (Previously 0 Active)
							AND sp.SUSPEND_ACCOUNT = 0
							AND bu.PRIMARY_EMAIL IS NULL"""

new_staff_only = """							SELECT sp.EXTERNAL_UID
							, 'staff' 
							FROM staff_py AS sp
							LEFT OUTER JOIN bixby_user AS bu
								ON sp.EXTERNAL_UID = bu.EXTERNAL_UID
									AND bu.USER_TYPE = 'staff'
							WHERE sp.EXTERNAL_USERSTATUS = 1 -- Changed Default Value True/Active (Previously 0 Active)
							AND sp.SUSPEND_ACCOUNT = 0
							AND bu.PRIMARY_EMAIL IS NULL"""

## Previous Testing Queries unsure if they are still necessary					

get_staff_from_ps = """SELECT t.USERS_DCID EXTERNAL_UID
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


get_students_from_ps = """SELECT s.id EXTERNAL_UID
						, s.SCHOOLID
						, s.STUDENT_NUMBER
						, s.FIRST_NAME GIVEN_NAME
						, s.LAST_NAME FAMILY_NAME
						, s.MIDDLE_NAME
						, s.DOB
						, CASE WHEN UPPER(s.gender) NOT IN ('M','F') THEN 'U' ELSE UPPER(s.gender) END GENDER
						, s.GRADE_LEVEL
						, s.HOME_ROOM
						, ps_customfields.getcf('students',s.id,'Area') AREA
						, TO_DATE(s.entrydate) ENTRYDATE
						, TO_DATE(s.exitdate) EXITDATE
						, CASE WHEN s.enroll_status = 0 THEN 0 ELSE 2 END EXTERNAL_USERSTATUS
						, CASE WHEN ps_customfields.getcf('students',s.id,'BUSD_Gmail_Suspended')=1 THEN 1
							ELSE 0 END SUSPEND_ACCOUNT
						      -- The Student Web ID and Parent Web_ID are here for other purposes maybe I will put that into a custom table
						, ps_customfields.getcf('students',s.id,'BUSD_Email_Override') EMAIL_OVERRIDE -- Pull from field BUSD_EMAIL_OVERRIDE
						, s.STUDENT_WEB_ID
						, s.WEB_ID PARENT_WEB_ID
						, s.STUDENT_ALLOWWEBACCESS
						, s.ALLOWWEBACCESS PARENT_ALLOWWEBACCESS
						FROM students s
						-- WHERE s.id BETWEEN 5000 AND 5200 
						-- AND s.grade_level >= 3
						ORDER BY s.id
						"""


# MySQL Queries
insert_staff_py_ps = """INSERT INTO STAFF_PY_PS (EXTERNAL_UID
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



insert_students_py_ps = """INSERT INTO STUDENTS_PY_PS (EXTERNAL_UID
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
											, STUDENT_ALLOWWEBACCESS
											, PARENT_ALLOWWEBACCESS
											) 
									VALUES (%s, %s, %s, %s, %s
											, %s, %s, %s, %s, %s
											, %s, %s, %s, %s, %s
											, %s, %s, %s, %s, %s)"""

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
										, bu.PRIMARY_EMAIL) AS PRIMARY_EMAIL
							, SP.GIVEN_NAME
							, SP.FAMILY_NAME
							, sp.EXTERNAL_UID
							, CASE WHEN sp.EXTERNAL_USERSTATUS + sp.SUSPEND_ACCOUNT >= 1 THEN 1 ELSE 0 END AS SUSPENDED
							, 0 AS CHANGE_PASSWORD
							, 1 GLOBAL_ADDRESSBOOK
							, '/Staff' AS OU_PATH
							-- , 'id:03dmca5l3d4ybj8' GOOGLE_OUID -- Staff OU

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
						, COALESCE(sp.EMAIL_OVERRIDE
									, bu.PRIMARY_EMAIL) AS PRIMARY_EMAIL
						, sp.GIVEN_NAME
						, sp.FAMILY_NAME
						, sp.EXTERNAL_UID
						, sp.SUSPEND_ACCOUNT AS SUSPENDED
						, 0 AS CHANGE_PASSWORD
						, 1 GLOBAL_ADDRESSBOOK
						, ou.OU_PATH
						-- , ou.GOOGLE_OUID

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
						, ou.OU_PATH
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
						, 1 AS CHANGE_PASSWORD
						, 1 GLOBAL_ADDRESSBOOK
						, '/Staff' AS OU_PATH
						FROM staff_py AS sp
						WHERE sp.EXTERNAL_UID = %s"""


new_staff_and_students_ps = """SELECT sp.EXTERNAL_UID 
							, 'student'
							FROM students_py AS sp
							LEFT OUTER JOIN bixby_user AS bu
								ON sp.EXTERNAL_UID = bu.EXTERNAL_UID
									AND bu.USER_TYPE = 'student'
							WHERE sp.EXTERNAL_USERSTATUS = 0
							AND sp.SUSPEND_ACCOUNT = 0
							AND bu.PRIMARY_EMAIL IS NULL
							AND current_date() BETWEEN sp.ENTRYDATE AND sp.EXITDATE

								UNION 

							SELECT sp.EXTERNAL_UID
							, 'staff' 
							FROM staff_py AS sp
							LEFT OUTER JOIN bixby_user AS bu
								ON sp.EXTERNAL_UID = bu.EXTERNAL_UID
									AND bu.USER_TYPE = 'staff'
							WHERE sp.EXTERNAL_USERSTATUS = 0
							AND sp.SUSPEND_ACCOUNT = 0
							AND bu.PRIMARY_EMAIL IS NULL"""


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

get_userinfo_vary_params = """SELECT bu.ID
						, bu.USER_TYPE
						, bu.PRIMARY_EMAIL
						, bu.GIVEN_NAME
						, bu.FAMILY_NAME
						, bu.EXTERNAL_UID
						, bu.SUSPENDED
						, bu.CHANGE_PASSWORD
						, bu.GLOBAL_ADDRESSBOOK
						, bu.OU_PATH
						-- , ou.GOOGLE_OUID
						
						FROM bixby_user AS bu
						JOIN orgunit AS ou
							ON bu.ou_path = ou.OU_PATH
						%s """

