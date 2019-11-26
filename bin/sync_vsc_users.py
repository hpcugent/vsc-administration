#!/usr/bin/env python
#
# Copyright 2013-2019 Ghent University
#
# This file is part of vsc-administration,
# originally created by the HPC team of Ghent University (http://ugent.be/hpc/en),
# with support of Ghent University (http://ugent.be/hpc),
# the Flemish Supercomputer Centre (VSC) (https://www.vscentrum.be),
# the Flemish Research Foundation (FWO) (http://www.fwo.be/en)
# and the Department of Economy, Science and Innovation (EWI) (http://www.ewi-vlaanderen.be/en).
#
# https://github.com/hpcugent/vsc-administration
#
# All rights reserved.
#
"""
This script synchronises the users and VO's from the HPC account page to the central
UGent storage for home and data.

For each (active) user, the following tasks are done:
    - create a directory in the home filesystem
    - chown this directory to the user
    - create the basic directories and scripts if they do not yet exist (.ssh, .bashrc, ...)
    - drop the user's public keys in the appropriate location
    - chmod the files to the correct value
    - chown the files (only changes things upon first invocation and new files)

The script should result in an idempotent execution, to ensure nothing breaks.
"""

import logging

from vsc.accountpage.sync import Sync
from vsc.administration.user import process_users, process_users_quota
from vsc.administration.vo import process_vos
from vsc.config.base import GENT
from vsc.utils import fancylogger

NAGIOS_HEADER = "sync_vsc_users"
NAGIOS_CHECK_INTERVAL_THRESHOLD = 15 * 60  # 15 minutes

SYNC_TIMESTAMP_FILENAME = "/var/cache/%s.timestamp" % (NAGIOS_HEADER)
SYNC_VSC_USERS_LOGFILE = "/var/log/%s.log" % (NAGIOS_HEADER)

logger = fancylogger.getLogger()
fancylogger.logToScreen(True)
fancylogger.setLogLevelInfo()

STORAGE_USERS_LIMIT_WARNING = 1
STORAGE_USERS_LIMIT_CRITICAL = 10
STORAGE_QUOTA_LIMIT_WARNING = 1
STORAGE_QUOTA_LIMIT_CRITICAL = 5
STORAGE_VO_LIMIT_WARNING = 1
STORAGE_VO_LIMIT_CRITICAL = 2


class UserGroupStatusUpdateError(Exception):
    pass


class VscUserSync(Sync):

    CLI_OPTIONS = {
        'nagios-check-interval-threshold': NAGIOS_CHECK_INTERVAL_THRESHOLD,
        'storage': ('storage systems on which to deploy users and vos', None, 'extend', []),
        'user': ('process users', None, 'store_true', False),
        'vo': ('process vos', None, 'store_true', False),
        'access_token': ('OAuth2 token to access the account page REST API', None, 'store', None),
        'account_page_url': ('URL of the account page where we can find the REST API', None, 'store', None),
        'host_institute': ('Name of the institute where this script is being run', str, 'store', GENT),
        'start_timestamp': ('Timestamp to start the sync from', str, 'store', None),
    }

    def do(self, dry_run):
        """
        Actual work
        - build the filter
        - fetches the users
        - process the users
        - write the new timestamp if everything went OK
        - write the nagios check file
        """

        stats = {}
        institute = self.options.host_institute

        (users_ok, users_fail) = ([], [])
        (quota_ok, quota_fail) = ([], [])
        if self.options.user:
            changed_accounts, _ = self.apc.get_accounts()  # we ignore inactive accounts

            logging.info("Found %d %s accounts that have changed in the accountpage since %s" %
                        (len(changed_accounts), institute, self.start_timestamp))

            for storage_name in self.options.storage:
                (users_ok, users_fail) = process_users(
                    changed_accounts,
                    storage_name,
                    self.apc,
                    institute,
                    self.options.dry_run)
                stats["%s_users_sync" % (storage_name,)] = len(users_ok)
                stats["%s_users_sync_fail" % (storage_name,)] = len(users_fail)
                stats["%s_users_sync_fail_warning" % (storage_name,)] = STORAGE_USERS_LIMIT_WARNING
                stats["%s_users_sync_fail_critical" % (storage_name,)] = STORAGE_USERS_LIMIT_CRITICAL

            for storage_name in self.options.storage:
                storage_changed_quota = [q for q in self.get_user_storage_quota(storage_name=storage_name)
                    if q.fileset.startswith('vsc')]
                logging.info("Found %d quota changes on storage %s in the accountpage",
                            len(storage_changed_quota), storage_name)
                (quota_ok, quota_fail) = process_users_quota(
                    storage_changed_quota,
                    storage_name,
                    self.apc,
                    institute,
                    self.options.dry_run)
                stats["%s_quota_sync" % (storage_name,)] = len(quota_ok)
                stats["%s_quota_sync_fail" % (storage_name,)] = len(quota_fail)
                stats["%s_quota_sync_fail_warning" % (storage_name,)] = STORAGE_QUOTA_LIMIT_WARNING
                stats["%s_quota_sync_fail_critical" % (storage_name,)] = STORAGE_QUOTA_LIMIT_CRITICAL

        (vos_ok, vos_fail) = ([], [])
        if self.options.vo:
            # FIXME: when api has changed, limit to modified per institute here
            changed_groups, _ = self.apc.get_groups()  # we ignore inactive groups
            changed_vos = [g for g in changed_groups if g.vsc_id.startswith("gvo") and not g.vsc_id.startswith("gvos")]
            changed_vo_quota = [q for q in self.apc.get_vo_storage_quota(storage_name=storage_name)
                if q.fileset.startswith('gvo') and not q.fileset.startswith('gvos')]

            vos = sorted(set([v.vsc_id for v in changed_vos] + [v.virtual_organisation for v in changed_vo_quota]))

            logging.info("Found %d %s VOs that have changed in the accountpage since %s" %
                        (len(changed_vos), institute, self.start_timestamp))
            logging.info("Found %d %s VOs that have changed quota in the accountpage since %s" %
                        (len(changed_vo_quota), institute, self.start_timestamp))
            logging.debug("Found the following {institute} VOs: {vos}".format(institute=institute, vos=vos))

            for storage_name in self.options.storage:
                (vos_ok, vos_fail) = process_vos(
                    vos,
                    storage_name,
                    self.apc,
                    self.start_timestamp,
                    institute,
                    self.options.dry_run)
                stats["%s_vos_sync" % (storage_name,)] = len(vos_ok)
                stats["%s_vos_sync_fail" % (storage_name,)] = len(vos_fail)
                stats["%s_vos_sync_fail_warning" % (storage_name,)] = STORAGE_VO_LIMIT_WARNING
                stats["%s_vos_sync_fail_critical" % (storage_name,)] = STORAGE_VO_LIMIT_CRITICAL

        if users_fail or quota_fail or vos_fail:
            return users_fail + quota_fail + vos_fail
        else:
            return False

if __name__ == '__main__':
    VscUserSync().main()
