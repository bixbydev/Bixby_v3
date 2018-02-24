#!/usr/bin/env python

# Filename: isiupdates.py

#=====================================================================#
# Copyright (c) 2017 Bradley Hilton <bradleyhilton@bradleyhilton.com> #
# Distributed under the terms of the GNU GENERAL PUBLIC LICENSE V3.   #
#=====================================================================#

import os
import database.mysql.base
import database.postgres.base
from logger.log import log
import util
from config.config import PRIVATE_DIRECTORY, CUSTOM_PARAMETERS


new_emails_query = """SELECT sp.EXTERNAL_UID
, bu.PRIMARY_EMAIL
, sp.STUDENT_NUMBER
FROM students_py AS sp
JOIN bixby_user AS bu
	ON sp.EXTERNAL_UID = bu.EXTERNAL_UID
    AND bu.USER_TYPE = 'student'
WHERE sp.EXTERNAL_USERSTATUS = 1
	AND (sp.STUDENT_WEB_ID IS NULL
		OR bu.PRIMARY_EMAIL != sp.STUDENT_WEB_ID);"""


student_portal_query = """SELECT s.local_student_id
-- , s.student_id
-- , s.email
, 1 AS ENABLE_PORTAL
FROM public.students AS s
JOIN public.student_session_aff AS ssa
    ON s.student_id = ssa.student_id
        AND current_date BETWEEN ssa.entry_date AND ssa.leave_date
JOIN public.grade_levels AS gl
    ON ssa.grade_level_id = gl.grade_level_id
LEFT OUTER JOIN portal.portal_students AS ps
    ON s.student_id = ps.student_id
JOIN sessions AS ses
    ON ssa.session_id = ses.session_id
JOIN sites AS sch
    ON ses.site_id = sch.site_id
WHERE gl.short_name IN ('6','7','8','9','10','11','12')
    AND s.email IS NOT NULL
    AND ps.student_id IS NULL
    AND sch.exclude_from_state_reporting = false"""



def get_address_list(bixby_cursor):
	bixby_cursor.execute(new_emails_query)
	return bixby_cursor.fetchall()


def update_isi_student_email(isi_cursor, update_list):
	log.info('Updating Student Emails in ISI')
	log.info(update_list)
	sql_query = """UPDATE public.students SET email = %s WHERE public.students.student_id = %s;"""
	for row in update_list:
		update = (row[1], row[0])
		print update
		isi_cursor.execute(sql_query, update)


def update_isi_student_portal_accounts(isi_cursor):
	hostname = CUSTOM_PARAMETERS.get('isi_sftp_host')
	username = CUSTOM_PARAMETERS.get('isi_sftp_user')
	password = CUSTOM_PARAMETERS.get('isi_sftp_password')
	remote_path = CUSTOM_PARAMETERS.get('isi_sftp_path')
	file_name = CUSTOM_PARAMETERS.get('isi_student_portal_accounts_file')
	local_file_path = os.path.join(PRIVATE_DIRECTORY, file_name)
	# Generate the Tab Delimited File and save it in the private directory
	util.csv_from_sql(student_portal_query, PRIVATE_DIRECTORY, file_name, isi_cursor, header=True, file_delimiter='	')
	util.sftp_file(username, password, hostname, local_file_path, remote_path, file_name)



def main():
	mcon = database.mysql.base.CursorWrapper()
	bixby_cursor = mcon.cursor
	pcon = database.postgres.base.CursorWrapper()
	isi_cursor = pcon.cursor

	update_list = get_address_list(bixby_cursor)
	update_isi_student_email(isi_cursor, update_list)
	update_isi_student_portal_accounts(isi_cursor)

	mcon.close()
	pcon.connection.commit()
	pcon.close()


	
	


if __name__ == '__main__':
	main()