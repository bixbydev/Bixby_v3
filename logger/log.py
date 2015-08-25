#!/usr/bin/env python

# Filename: log.py

#=====================================================================#
# Copyright (c) 2015 Bradley Hilton <bradleyhilton@bradleyhilton.com> #
# Distributed under the terms of the GNU GENERAL PUBLIC LICENSE V3.   #
#=====================================================================#

import time
import logging
import logging.handlers


# The old Logging Facility works beautifully!
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
# log.setLevel(logging.INFO)
formatter = logging.Formatter(
		"""%(asctime)s - [%(levelname)s] - %(filename)s - %(message)s""")

# Log to File/File Handler
fhandler = logging.FileHandler('logs/Bixby.log')
fhandler.setFormatter(formatter)
log.addHandler(fhandler)

#stdout Log Handler/Log to stdout
shandler = logging.StreamHandler()
shandler.setFormatter(formatter)
log.addHandler(shandler)
