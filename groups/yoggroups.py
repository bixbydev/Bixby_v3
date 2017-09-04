import time

import util
import database.mysql.base
import database.oracle.base
# from gservice.groups import BatchGroups, BatchMembers

import gservice.groups


CALCULATION_YEAR = 2030



new_yog_groups = """SELECT LOWER(CONCAT('y', sites.ABBREVIATION, '-classof-'
                                , {0} - (dpt.unit), '-students@berkeley.net')
                                ) AS GROUP_EMAIL
    , CONCAT(sites.DESCRIPTION, ' Class of ', {0} - (dpt.unit)) AS GROUP_NAME
    , CONCAT(sites.DESCRIPTION, ' Class of ', {0} - (dpt.unit)) AS GROUP_DESCRIPTION
    , {0} - dpt.UNIQUE_IDENTIFIER AS UNIQUE_ATTRIBUTE
    , dpt.SITECODE AS DEPARTMENT_ID
    , 'StuSchoolYOG' AS GROUP_TYPE
FROM departments AS dpt
JOIN sites
    ON dpt.SITEID = sites.siteid
LEFT OUTER JOIN groups AS g
    ON {0} - dpt.UNIQUE_IDENTIFIER = g.UNIQUE_ATTRIBUTE
        AND dpt.sitecode = g.DEPARTMENT_ID
WHERE g.GROUP_EMAIL IS NULL
    AND sites.AUTO_EXCLUDE = 0
    AND sites.UPPER_UNIT > 5
    AND dpt.SITECODE != 6504""".format(CALCULATION_YEAR)


stale_yog_members = """SELECT gm.GOOGLE_GROUPID
, gm.GOOGLE_USERID 
-- , bu.EXTERNAL_UID
-- , g.GROUP_EMAIL
-- , g.UNIQUE_ATTRIBUTE
-- , g.DEPARTMENT_ID
-- , g.*
-- , student.*
FROM groups AS g

JOIN group_member AS gm
	ON gm.GOOGLE_GROUPID = g.GOOGLE_GROUPID
JOIN bixby_user bu
	ON gm.GOOGLE_USERID = bu.GOOGLE_ID
LEFT OUTER JOIN (SELECT sp.SCHOOLID
					, {0} - sp.GRADE_LEVEL AS UNIQUE_ATTRIBUTE
					, bu.GOOGLE_ID
                    
				FROM students_py AS sp
				JOIN bixby_user AS bu
					ON sp.EXTERNAL_UID = bu.EXTERNAL_UID
				AND bu.USER_TYPE = 'student') AS student
					ON g.UNIQUE_ATTRIBUTE = student.UNIQUE_ATTRIBUTE
						AND g.department_id = student.schoolid
						AND gm.GOOGLE_USERID = student.google_id
WHERE g.group_type = 'StuSchoolYOG'
	AND student.google_id IS NULL
    """.format(CALCULATION_YEAR)


new_yog_members = """SELECT g.GOOGLE_GROUPID
, bu.GOOGLE_ID
, 'MEMBER' AS role
, g.GROUP_EMAIL
, bu.GIVEN_NAME
, bu.EXTERNAL_UID
FROM students_py AS sp
JOIN bixby_user AS bu
	ON sp.EXTERNAL_UID = bu.EXTERNAL_UID
	AND bu.USER_TYPE = 'student'
JOIN groups AS g
	ON sp.schoolid = g.DEPARTMENT_ID
		AND {0} - sp.GRADE_LEVEL = g.UNIQUE_ATTRIBUTE
LEFT OUTER JOIN group_member AS gm
	ON bu.google_id = gm.GOOGLE_USERID
		AND g.GOOGLE_GROUPID = gm.GOOGLE_GROUPID

WHERE gm.id IS NULL
AND curdate() BETWEEN sp.ENTRYDATE AND sp.EXITDATE
AND sp.EXTERNAL_USERSTATUS = 1
AND sp.GRADE_LEVEL >= 9;
""".format(CALCULATION_YEAR)


def main():
	gservice.groups.insert_new_groups(new_yog_groups)
	#gservice.groups.delete_group_members(stale_yog_members)
	gservice.groups.insert_new_group_members(new_yog_members)
	

if __name__ == '__main__':
	main()
