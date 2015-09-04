
#!/usr/bin/python

#------------------------------------------------------------------------------
#       Copyright (C) 2013 Bradley Hilton <bradleyhilton@bradleyhilton.com>
#
#  Distributed under the terms of the GNU GENERAL PUBLIC LICENSE V3. 
#______________________________________________________________________________

# There is stuff below you may need to change. Specifically in the Oracle, MySQL, And Google Provisioning API Stuff sections.

# Filename: student_portal_logins.py

import os
from config import config
import database.mysql.base
import util

new_portal_usernames = """SELECT sp.student_number
	, LEFT(SUBSTRING_INDEX(bu.PRIMARY_EMAIL, '@', 1), 20) AS Student_Web_ID
	, CONCAT('Berkeley', sp.student_number) AS Student_Web_Password
	, CASE WHEN sp.grade_level < 6 THEN '0' ELSE '1' END AS Student_AllowWebAccess
	, CASE WHEN sp.grade_level < 6 THEN '0' ELSE '1' END AS AllowWebAccess
	, REPLACE(CONCAT(bu.GIVEN_NAME, DATE_FORMAT(sp.dob, '%m%d%y')), ' ', '') AS Web_Password
	, LEFT(REPLACE(CONCAT(bu.GIVEN_NAME, CAST(sp.student_number AS CHAR)), ' ', ''), 20) AS WEB_ID
	, bu.PRIMARY_EMAIL AS BUSD_Email_Address
	
FROM bixby_user AS bu
JOIN students_py AS sp
	ON bu.EXTERNAL_UID = sp.EXTERNAL_UID
		AND bu.USER_TYPE = 'Student'
		
WHERE (sp.student_web_id IS NULL OR sp.parent_web_id IS NULL)
	AND sp.grade_level > 1
ORDER BY bu.family_name"""

file_output_path = config.PRIVATE_DIRECTORY
file_name = 'StudentPortalLogins.csv'

def main():
	mcon = database.mysql.base.CursorWrapper()
	mcurs = mcon.cursor
	util.csv_from_sql(new_portal_usernames, file_output_path, file_name, mcurs, header=True)

	username = config.PS_PASSWORDS_USER
	password = config.PS_PASSWORDS_PASSWORD
	hostname = config.PS_PASSWORDS_HOSTNAME
	local_path = os.path.join(file_output_path, file_name)
	remote_path = config.PS_PASSWORDS_REMOTE_PATH

	util.sftp_file(username, password, hostname, local_path, remote_path, file_name)


if __name__ == '__main__':
	main()
