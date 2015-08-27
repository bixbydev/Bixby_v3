#!/usr/bin/env python

# Filename: sectiongroups.py

#=====================================================================#
# Copyright (c) 2015 Bradley Hilton <bradleyhilton@bradleyhilton.com> #
# Distributed under the terms of the GNU GENERAL PUBLIC LICENSE V3.   #
#=====================================================================#


import util
import database.mysql.base
import database.oracle.base




get_sections_from_ps = """SELECT sec.id sectionid
, sec.schoolid
, 'z'||sch.abbreviation||'-'
	||SUBSTR(tr.abbreviation, 1,2)||'-'
	||SUBSTR(t.first_name, 1, 1)||'-'
	||REGEXP_REPLACE(t.last_name, '[[:punct:][:space:]]')
	||CASE WHEN sch.high_grade > 5 THEN '-'||REGEXP_REPLACE(p.abbreviation, '(\+)', 'p') ELSE '' END
	||'@berkeley.net' GROUP_EMAIL
  
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
, sec.teacher GROUP_OWNER
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
WHERE ABS(termid) BETWEEN 2400 AND 2410
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

