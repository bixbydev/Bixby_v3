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
# from gservice.groups import BatchGroups, BatchMembers

import gservice.groups

NOT_DONE_new_course_groups = """
SELECT sp.GROUP_EMAIL
, CONCAT('z-bhs-', LOWER(sp.COURSE_NAME), '@berkeley.net') AS GROUP_EMAIL1
, sp.GROUP_NAME
, sp.GROUP_DESCRIPTION
, sp.SECTIONID AS UNIQUE_ATTRIBUTE
, sp.schoolid AS DEPARTMENT_ID
, 'StudentSection' AS GROUP_TYPE

FROM sections_py AS sp
LEFT OUTER JOIN groups g
	ON sp.SECTIONID = g.UNIQUE_ATTRIBUTE
		AND g.GROUP_TYPE = 'Courses'
WHERE sp.COURSE_NUMBER IN ('AC01Y')
AND g.GROUP_EMAIL IS NULL;"""

NOT_DONE_new_members = """SELECT g.GOOGLE_GROUPID
, bu.GOOGLE_ID
, 'MEMBER' AS role
, g.GROUP_EMAIL
, bu.GIVEN_NAME
, bu.EXTERNAL_UID
FROM studentschedule_py AS ssp
JOIN groups AS g
	ON ssp.sectionid = g.unique_attribute
		AND g.GROUP_TYPE = 'Courses'
JOIN bixby_user AS bu
	ON ssp.studentid = bu.EXTERNAL_UID
		AND bu.USER_TYPE = 'student'
LEFT OUTER JOIN group_member AS gm
	ON g.GOOGLE_GROUPID = gm.GOOGLE_GROUPID
		AND bu.GOOGLE_ID = gm.GOOGLE_USERID
WHERE gm.GOOGLE_GROUPID IS NULL
	AND curdate() BETWEEN ssp.dateenrolled AND ssp.dateleft
"""
