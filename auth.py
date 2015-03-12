#!/usr/bin/python

# Notes: To-Do: Remove 20150310
# PyDocs https://google-api-client-libraries.appspot.com/documentation/admin/directory_v1/python/latest/admin_directory_v1.users.html
# The JSON request https://developers.google.com/admin-sdk/directory/v1/guides/manage-users#create_user
# This script https://developers.google.com/admin-sdk/directory/v1/quickstart/quickstart-python#step_2_install_the_google_client_library

import httplib2

from apiclient import errors
# from apiclient.discovery import build
from oauth2client.client import OAuth2WebServerFlow

from config import config

CLIENT_ID = config.CLIENT_ID
CLIENT_SECRET = config.CLIENT_SECRET

# Check https://developers.google.com/admin-sdk/directory/v1/guides/authorizing for all available scopes
OAUTH_SCOPE = """https://www.googleapis.com/auth/admin.directory.user\
 https://www.googleapis.com/auth/admin.directory.group\
 https://www.googleapis.com/auth/admin.directory.device.chromeos\
 https://www.googleapis.com/auth/admin.directory.orgunit"""

# Redirect URI for installed apps
# You must create a secret for native application
REDIRECT_URI = 'urn:ietf:wg:oauth:2.0:oob'
# REDIRECT_URI = 'urn:ietf:wg:oauth:2.0:oob:auto' # Does not prompt for code?

# Run through the OAuth flow and retrieve credentials
flow = OAuth2WebServerFlow(CLIENT_ID, CLIENT_SECRET, OAUTH_SCOPE, REDIRECT_URI)
authorize_url = flow.step1_get_authorize_url()
print 'Go to the following link in your browser: ' + authorize_url
code = raw_input('Enter verification code: ').strip()
credentials = flow.step2_exchange(code)


# Create an httplib2.Http object and authorize it with our credentials
http = httplib2.Http()
http = credentials.authorize(http)

print config.PRIMARY_DOMAIN