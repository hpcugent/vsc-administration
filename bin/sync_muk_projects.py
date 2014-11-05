#!/usr/bin/env python
##
#
# Copyright 2012-2013 Ghent University
#
# This file is part of the tools originally by the HPC team of
# Ghent University (http://ugent.be/hpc).
#
# This is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation v2.
"""
This script checks the project entries in the LDAP that have changed since a given timestamp
and that are in the muk autogroup.

For these, the home and other shizzle should be set up.

@author Andy Georges
"""

import os
import sys

from vsc.administration.group import Group
from vsc.config.base import Muk, ANTWERPEN, BRUSSEL, GENT, LEUVEN
from vsc.ldap.configuration import VscConfiguration
from vsc.ldap.filters import CnFilter, InstituteFilter, LdapFilter
from vsc.ldap.timestamp import convert_timestamp, write_timestamp
from vsc.ldap.utils import LdapQuery
from vsc.utils import fancylogger
from vsc.utils.nagios import NAGIOS_EXIT_CRITICAL
from vsc.utils.script_tools import ExtendedSimpleOption

NAGIOS_HEADER = 'sync_muk_projects'
NAGIOS_CHECK_INTERVAL_THRESHOLD = 24 * 60 * 60 # 1 day

SYNC_TIMESTAMP_FILENAME = "/var/run/%s.timestamp" % (NAGIOS_HEADER)
SYNC_MUK_PROJECTS_LOCKFILE = "/gpfs/scratch/user/%s.lock" % (NAGIOS_HEADER)

logger = fancylogger.getLogger(__name__)
fancylogger.logToScreen(True)
fancylogger.setLogLevelInfo()


def process_project(options, project):

    muk = Muk()  # Singleton class, so no biggie
    logger.info("Processing project %{project_id}".format(project_id=project.project_id))

    try:
        project.create_scratch_fileset()
        return True
    except:
        logger.exception("Cannot create scratch fileset for project %{project_id}".format(project_id=project.project_id,))
        return False


def main():
    """
    Main script.
    - loads the previous timestamp
    - build the filter
    - fetches the users
    - process the users
    - write the new timestamp if everything went OK
    - write the nagios check file
    """

    options = {
        'nagios-check-interval-threshold': NAGIOS_CHECK_INTERVAL_THRESHOLD,
        'locking': SYNC_MUK_PROJECTS_LOCKFILE,
    }

    opts = ExtendedSimpleOption(options)
    stats = {}
    opts.epilogue(stats)


if __name__ == '__main__':
    main()
