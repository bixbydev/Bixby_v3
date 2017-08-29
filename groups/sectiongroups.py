#!/usr/bin/env python

# Filename: sectiongroups.py

#=====================================================================#
# Copyright (c) 2015 Bradley Hilton <bradleyhilton@bradleyhilton.com> #
# Distributed under the terms of the GNU GENERAL PUBLIC LICENSE V3.   #
#=====================================================================#

# import time

import util
import database.mysql.base
# import database.oracle.base
import database.postgres.base
# from gservice.groups import BatchGroups, BatchMembers

import gservice.groups

START_TERM = 2600
END_TERM = START_TERM + 10

get_illuminate_sections = """WITH school_map AS (SELECT sch.site_id
, CASE WHEN sch.site_id = 6097729 THEN 'BAM'
	WHEN sch.site_id = 131177 THEN 'BHS' 
    WHEN sch.site_id = 134924 THEN 'BTA'
    WHEN sch.site_id = 122804 THEN 'Pre'
    WHEN sch.site_id = 6090195 THEN 'Crag'
    WHEN sch.site_id = 6090211 THEN 'Emer'
    WHEN sch.site_id = 6090252 THEN 'Jeff'
    WHEN sch.site_id = 6105316 THEN 'JM'
    WHEN sch.site_id = 6090278 THEN 'LeCo'
    WHEN sch.site_id = 6090294 THEN 'Long'
    WHEN sch.site_id = 6090286 THEN 'MX'
    WHEN sch.site_id = 6056857 THEN 'King'
    WHEN sch.site_id = 6504 THEN 'NPS'
    WHEN sch.site_id = 6090302 THEN 'Oxfo'
    WHEN sch.site_id = 6090187 THEN 'Rosa'
    WHEN sch.site_id = 6090310 THEN 'TO'
    WHEN sch.site_id = 6090328 THEN 'Wash'
    WHEN sch.site_id = 6056865 THEN 'Will'
    WHEN sch.site_id = 9999999 THEN 'DO'
    WHEN sch.site_id = 999999999 THEN 'CES'
    WHEN sch.site_id = 10000000 THEN 'PVTMS'
    WHEN sch.site_id = 20000000 THEN 'Inact'
    WHEN sch.site_id = 999999 THEN 'Grad'
    ELSE NULL
    END SiteShortName
, sch.site_name
FROM sites sch)

SELECT DISTINCT st.section_id AS SECTIONID

, sch.site_id AS SCHOOLID
, LOWER('z'||sm.SiteShortName||'-'
	||CASE WHEN COUNT(*) OVER (PARTITION BY st.section_id) > 1 THEN 'Y' ELSE SUBSTR(tr.term_name, 1,2) END||'-'
	||SUBSTR(u.first_name, 1, 1)||'-'
	||REGEXP_REPLACE(u.last_name, '\W+', '','g'))
	||CASE WHEN gl.short_name IN ('6','7','8','9','10','11','12') THEN '-'||REGEXP_REPLACE(LOWER(tb.timeblock_name), '\W+', '','g') ELSE '' END
	||'@berkeley.net' GROUP_EMAIL
, 'z '||sm.SiteShortName||' '
	||CASE WHEN COUNT(*) OVER (PARTITION BY st.section_id) > 1 THEN 'Y' ELSE SUBSTR(tr.term_name, 1,2) END||' '
	||SUBSTR(u.first_name, 1, 1)||'-'
	||u.last_name
	||CASE WHEN gl.short_name IN ('6','7','8','9','10','11','12') THEN '-'||REGEXP_REPLACE(LOWER(tb.timeblock_name), '\W+', '', 'g') ELSE '' END Group_Name

, sm.SiteShortName||' '
  ||u.Last_Name||' '
  ||CASE WHEN gl.short_name IN ('6','7','8','9','10','11','12') THEN crs.short_name
    ELSE tr.term_name||' Period '||REGEXP_REPLACE(tb.timeblock_name, '\W+', '','g') END GROUP_DESCRIPTION
    
-- , tr.term_id AS TERMID
, u.user_id AS GROUP_OWNER
-- , u.local_user_id
-- , u.last_name
-- , u.first_name
, crs.school_course_id AS COURSE_NUMBER
, st.section_id AS SECTION_NUMBER

, crs.short_name AS COURSE_NAME
--, tb.timeblock_name Period
, CASE WHEN COUNT(*) OVER (PARTITION BY st.section_id) > 1 THEN 'Y' ELSE SUBSTR(tr.term_name, 1,2) END


FROM  section_teacher_aff st
	JOIN section_term_aff sta
    	ON st.section_id = sta.section_id
    JOIN terms tr
    	ON tr.term_id = sta.term_id
    JOIN sessions ses
    	ON ses.session_id = tr.session_id
	JOIN sites sch
    	ON sch.site_id = ses.site_id
    LEFT OUTER JOIN grade_levels gl
		ON gl.grade_level_id = sch.start_grade_level_id
	JOIN section_course_aff sca
		ON sca.section_id = st.section_id
	JOIN courses crs
		ON crs.course_id = sca.course_id
    JOIN section_timeblock_aff stb
    	ON stb.section_id = st.section_id
    JOIN timeblocks tb
    	ON tb.timeblock_id = stb.timeblock_id
	JOIN users u
		ON u.user_id = st.user_id
    JOIN school_map AS sm
        ON sch.site_id = sm.site_id

WHERE ses.academic_year = 2018
AND gl.short_name IN ('TK','K','1','2','3','4','5','6','7','8','9','10','11','12')
AND sch.exclude_from_state_reporting = '0'

-- AND u.user_id = 43

ORDER BY sectionid, group_email;
"""

get_illuminate_schedules = """SELECT ssa.ssa_id as ps_id
, ssa.student_id AS studentid
, ssa.section_id AS sectionid
, ses.site_id AS schoolid
, ssa.entry_date AS dateenrolled
, CASE WHEN ssa.leave_date IS NULL THEN tr.end_date ELSE ssa.leave_date END AS dateleft
FROM section_student_aff AS ssa
JOIN section_term_aff AS sta
    ON ssa.section_id = sta.section_id
JOIN terms AS tr
    ON sta.term_id = tr.term_id
JOIN sessions AS ses
    ON tr.session_id = ses.session_id
-- WHERE current_date BETWEEN tr.start_date AND tr.end_date
WHERE '2017-08-29' BETWEEN tr.start_date AND tr.end_date"""



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
  
WHERE sec.termid BETWEEN 2600 AND 2610
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
WHERE ABS(termid) BETWEEN 2600 AND 2610
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
	AND curdate() BETWEEN ssp.dateenrolled AND ssp.dateleft
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

stale_group_members = """SELECT gm.GOOGLE_GROUPID
-- THIS QUERY DOES NOT WORK YET 
-- THIS QUERY DOES NOT WORK YET 
-- THIS QUERY DOES NOT WORK YET 
-- THIS QUERY DOES NOT WORK YET It may remove users from groups with more than one section
, gm.GOOGLE_USERID
, bu.PRIMARY_EMAIL
, g.*
FROM group_member AS gm
JOIN groups AS g
	ON gm.GOOGLE_GROUPID = g.GOOGLE_GROUPID
    AND g.GROUP_TYPE = 'StudentSection'
    AND gm.ROLE = 'MEMBER'
JOIN bixby_user AS bu
	ON gm.GOOGLE_USERID = bu.GOOGLE_ID
LEFT OUTER JOIN studentschedule_py AS sp
	ON g.UNIQUE_ATTRIBUTE = sp.SECTIONID 
		AND bu.EXTERNAL_UID = sp.studentid
        AND now() BETWEEN sp.dateenrolled AND sp.dateleft
WHERE sp.studentid IS NULL
-- THIS QUERY DOES NOT WORK YET 
"""


def refresh_section_groups_data():
	#database.mysql.base.backup_mysql()
	# Open an Oracle Cursor
	#ocon = database.oracle.base.CursorWrapper()
	#ocurs = ocon.cursor
	# Open Postgres Cursor
	# Postgres SIS Connection
	pcon = database.postgres.base.CursorWrapper()
	pc = pcon.cursor
	# Open a MySQL Cursor
	mcon = database.mysql.base.CursorWrapper()
	mcurs = mcon.cursor
	# refresh the sections data (groups)
	mcurs.execute('TRUNCATE TABLE sections_py')
	util.copy_foreign_table(pc, get_illuminate_sections, mcurs, 'sections_py')

	mcurs.execute('TRUNCATE TABLE studentschedule_py')
	util.copy_foreign_table(ps, get_illuminate_schedules, mcurs, 'studentschedule_py')

	mcon.close()
	pc.close()
	#ocon.close()


def main():
	refresh_section_groups_data()
	#gservice.groups.insert_new_groups(new_section_groups)
	#gservice.groups.insert_new_group_members(new_group_owners)
	#gservice.groups.delete_group_members(stale_group_owners)
	#gservice.groups.insert_new_group_members(new_student_group_members)
	

if __name__ == '__main__':
	main()

