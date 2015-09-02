#!/usr/bin/env python

# Filename: sectiongroups.py

#=====================================================================#
# Copyright (c) 2015 Bradley Hilton <bradleyhilton@bradleyhilton.com> #
# Distributed under the terms of the GNU GENERAL PUBLIC LICENSE V3.   #
#=====================================================================#

import time

import util
import database.mysql.base
import database.oracle.base
from gservice.groups import BatchGroups, BatchMembers


get_sections_from_ps = """SELECT sec.id sectionid
, sec.schoolid
, LOWER('z'||sch.abbreviation||'-'
	||SUBSTR(tr.abbreviation, 1,2)||'-'
	||SUBSTR(t.first_name, 1, 1)||'-'
	||REGEXP_REPLACE(t.last_name, '[[:punct:][:space:]]')
	||CASE WHEN sch.high_grade > 5 THEN '-'||REGEXP_REPLACE(p.abbreviation, '(\+)', 'p') ELSE '' END
	||'@berkeley.net') GROUP_EMAIL
  
, 'z '||sch.abbreviation||' '
||SUBSTR(tr.abbreviation, 1,2)||' '
||SUBSTR(t.first_name, 1, 1)||'-'
||t.last_name
||CASE WHEN sch.high_grade > 5 THEN ' - '||REGEXP_REPLACE(p.abbreviation, '(\+)', 'p') ELSE '' END Group_Name

, sch.abbreviation||' '
  ||t.Last_Name||' '
  ||CASE WHEN sch.high_grade=5 THEN crs.course_name
    ELSE tr.name||' Period '||REGEXP_REPLACE(p.abbreviation, '(\+)', 'p') END GROUP_DESCRIPTION

, sec.termid 
, t.users_dcid GROUP_OWNER -- EXTERNAL_UID
, sec.course_number
, sec.section_number
, crs.course_name

FROM sections sec
JOIN terms tr
  ON sec.termid = tr.id
    AND sec.schoolid = tr.schoolid
JOIN courses crs
  ON sec.course_number = crs.course_number
JOIN teachers t
  ON sec.teacher = t.id
JOIN period p 
  ON p.schoolid=sec.schoolid 
	AND p.year_id=SUBSTR(sec.termid, 0, 2) 
	AND p.period_number=SUBSTR(REGEXP_REPLACE(sec.expression, '[[:punct:][:alpha:]]'), 0 , 2)
JOIN schools sch
  ON sec.schoolid = sch.school_number
  
WHERE sec.termid BETWEEN 2500 AND 2510
AND (sch.high_grade > 5
OR sec.course_number LIKE '_000'
OR sec.course_number = 'SDC01')

"""

get_cc_schedule_from_ps = """SELECT id ps_id
, studentid
, sectionid
, termid
, schoolid
, dateenrolled
, dateleft
FROM cc
WHERE ABS(termid) BETWEEN 2500 AND 2501
"""

new_section_groups = """SELECT sp.GROUP_EMAIL
, sp.GROUP_NAME
, sp.GROUP_DESCRIPTION
, sp.SECTIONID AS UNIQUE_ATTRIBUTE
, sp.schoolid AS DEPARTMENT_ID
, 'StudentSection' AS GROUP_TYPE
FROM sections_py AS sp
LEFT OUTER JOIN groups g
	ON sp.SECTIONID = g.UNIQUE_ATTRIBUTE
		AND g.GROUP_TYPE = 'StudentSection'
WHERE g.GROUP_EMAIL IS NULL
	-- AND LOWER(sp.group_email) LIKE LOWER('zBHS-%')
"""

new_group_owners = """SELECT g.GOOGLE_GROUPID
, bu.GOOGLE_ID
, 'OWNER' AS role
, g.GROUP_EMAIL
FROM groups AS g
JOIN sections_py AS sp
	ON g.UNIQUE_ATTRIBUTE = sp.SECTIONID
		AND g.GROUP_TYPE = 'StudentSection'
JOIN bixby_user AS bu
	ON sp.GROUP_OWNER = bu.EXTERNAL_UID
		AND bu.USER_TYPE = 'staff'
LEFT OUTER JOIN group_member AS gm
	ON bu.GOOGLE_ID = gm.GOOGLE_USERID
		AND g.GOOGLE_GROUPID = gm.GOOGLE_GROUPID
        AND gm.ROLE = 'OWNER'

WHERE gm.GOOGLE_GROUPID IS NULL
	AND bu.GOOGLE_ID IS NOT NULL
-- LIMIT 40
"""

new_student_group_members = """SELECT g.GOOGLE_GROUPID
, bu.GOOGLE_ID
, 'MEMBER' AS role
, g.GROUP_EMAIL
, bu.GIVEN_NAME
, bu.EXTERNAL_UID
FROM studentschedule_py AS ssp
JOIN groups AS g
	ON ssp.sectionid = g.unique_attribute
		AND g.GROUP_TYPE = 'StudentSection'
JOIN bixby_user AS bu
	ON ssp.studentid = bu.EXTERNAL_UID
		AND bu.USER_TYPE = 'student'
LEFT OUTER JOIN group_member AS gm
	ON g.GOOGLE_GROUPID = gm.GOOGLE_GROUPID
		AND bu.GOOGLE_ID = gm.GOOGLE_USERID
WHERE gm.GOOGLE_GROUPID IS NULL
-- LIMIT 40
"""

update_section_groups = """SELECT g.GROUP_EMAIL
, sp.GROUP_EMAIL
, sp.GROUP_NAME
, sp.GROUP_DESCRIPTION
FROM groups AS g
JOIN sections_py AS sp
	ON g.UNIQUE_ATTRIBUTE = sp.SECTIONID
		AND g.GROUP_TYPE = 'StudentSection'
WHERE LOWER(g.GROUP_EMAIL) != LOWER(sp.GROUP_EMAIL)
"""

stale_group_owners = """SELECT gm.GOOGLE_GROUPID
, gm.GOOGLE_USERID
FROM group_member AS gm
JOIN groups AS g
	ON gm.GOOGLE_GROUPID = g.GOOGLE_GROUPID
    AND g.GROUP_TYPE = 'StudentSection'
    AND gm.ROLE = 'OWNER'
JOIN bixby_user AS bu
	ON gm.GOOGLE_USERID = bu.GOOGLE_ID
LEFT OUTER JOIN sections_py AS sp
	ON g.UNIQUE_ATTRIBUTE = sp.SECTIONID
		AND bu.EXTERNAL_UID = sp.GROUP_OWNER
WHERE sp.group_email IS NULL
"""


def refresh_section_groups_data():
	database.mysql.base.backup_mysql()
	# Open an Oracle Cursor
	ocon = database.oracle.base.CursorWrapper()
	ocurs = ocon.cursor
	# Open a MySQL Cursor
	mcon = database.mysql.base.CursorWrapper()
	mcurs = mcon.cursor
	# refresh the sections data (groups)
	mcurs.execute('TRUNCATE TABLE sections_py')
	util.copy_foreign_table(ocurs, get_sections_from_ps, mcurs, 'sections_py')

	mcurs.execute('TRUNCATE TABLE studentschedule_py')
	util.copy_foreign_table(ocurs, get_cc_schedule_from_ps, mcurs, 'studentschedule_py')

	mcon.close()
	ocon.close()


def insert_new_section_groups():
	bg = BatchGroups()
	bg.cursor.execute(new_section_groups)
	new_groups = bg.cursor.fetchall()
	fields = [i[0] for i in bg.cursor.description]
	chunked_groups = util.list_chunks(list(new_groups), 20)
	for chunk in chunked_groups:
		for group in chunk:
			# This stuff can be removed
			#group_object = dict(zip(fields, group))
			print '+' * 20
			print group
			bg.insert_group(email=group[0], 
							name=group[1], 
							description=group[2], 
							unique_attribute=group[3], 
							department_id=group[4],
                    		group_type=group[5] )
		time.sleep(3)
		bg.execute()


def insert_new_group_members(members_query):
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
	bg = BatchMembers()
	bg.cursor.execute(members_query)
	new_members = bg.cursor.fetchall()
	fields = [i[0] for i in bg.cursor.description]
	chunked_members = util.list_chunks(list(new_members), 20)
	for chunk in chunked_members:
		for mem in chunk:
			print mem
			bg.delete_member(mem[0], mem[1])
		time.sleep(2)
		bg.execute()


def main():
	refresh_section_groups_data()
	insert_new_section_groups()
	insert_new_group_members(new_group_owners)
	delete_group_members(stale_group_owners)
	insert_new_group_members(new_student_group_members)
	

if __name__ == '__main__':
	main()

