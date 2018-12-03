#!/usr/bin/env python
#
# Copyright 2013-2018 Ghent University
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

import sys
import datetime

from vsc.accountpage.client import AccountpageClient
from vsc.accountpage.wrappers import mkVscUserSizeQuota
from vsc.administration.user import process_users, process_users_quota
from vsc.administration.vo import process_vos
from vsc.config.base import GENT
from vsc.utils.dateandtime import utc
from vsc.utils import fancylogger
from vsc.utils.missing import nub
from vsc.utils.nagios import NAGIOS_EXIT_CRITICAL
from vsc.utils.script_tools import ExtendedSimpleOption
from vsc.utils.timestamp import convert_timestamp, read_timestamp, write_timestamp
from vsc.utils.timestamp import convert_to_unix_timestamp

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


def main():
    """
    Main script.
    - build the filter
    - fetches the users
    - process the users
    - write the new timestamp if everything went OK
    - write the nagios check file
    """

    options = {
        'nagios-check-interval-threshold': NAGIOS_CHECK_INTERVAL_THRESHOLD,
        'storage': ('storage systems on which to deploy users and vos', None, 'extend', []),
        'user': ('process users', None, 'store_true', False),
        'vo': ('process vos', None, 'store_true', False),
        'access_token': ('OAuth2 token to access the account page REST API', None, 'store', None),
        'account_page_url': ('URL of the account page where we can find the REST API', None, 'store', None),
        'host_institute': ('Name of the institute where this script is being run', str, 'store', GENT),
    }

    opts = ExtendedSimpleOption(options)
    stats = {}

    try:
        start_time = datetime.datetime.now(tz=utc) + datetime.timedelta(seconds=-10)
        client = AccountpageClient(token=opts.options.access_token, url=opts.options.account_page_url + "/api/")

        try:
            last_timestamp = read_timestamp(SYNC_TIMESTAMP_FILENAME)
        except Exception:
            logger.exception("Something broke reading the timestamp from %s" % SYNC_TIMESTAMP_FILENAME)
            last_timestamp = "200901010000Z"

        logger.info("Last recorded timestamp was %s" % (last_timestamp))
        last_timestamp = convert_to_unix_timestamp(last_timestamp)
        institute = opts.options.host_institute

        (users_ok, users_fail) = ([], [])
        (quota_ok, quota_fail) = ([], [])
        if opts.options.user:
            changed_accounts = client.account.institute[institute].modified[last_timestamp].get()[1]

            logger.info("Found %d %s accounts that have changed in the accountpage since %s" %
                        (len(changed_accounts), institute, last_timestamp))

            accounts = nub([u['vsc_id'] for u in changed_accounts])

            for storage_name in opts.options.storage:
                (users_ok, users_fail) = process_users(opts.options,
                                                       accounts,
                                                       storage_name,
                                                       client,
                                                       institute)
                stats["%s_users_sync" % (storage_name,)] = len(users_ok)
                stats["%s_users_sync_fail" % (storage_name,)] = len(users_fail)
                stats["%s_users_sync_fail_warning" % (storage_name,)] = STORAGE_USERS_LIMIT_WARNING
                stats["%s_users_sync_fail_critical" % (storage_name,)] = STORAGE_USERS_LIMIT_CRITICAL

            for storage_name in opts.options.storage:
                storage_changed_quota = [mkVscUserSizeQuota(q) for q in
                                         client.quota.user.storage[storage_name].modified[last_timestamp].get()[1]]
                storage_changed_quota = [q for q in storage_changed_quota if q.fileset.startswith('vsc')]
                logger.info("Found %d accounts that have changed quota on storage %s in the accountpage since %s",
                            len(storage_changed_quota), storage_name, last_timestamp)
                (quota_ok, quota_fail) = process_users_quota(opts.options,
                                                             storage_changed_quota,
                                                             storage_name,
                                                             client,
                                                             institute)
                stats["%s_quota_sync" % (storage_name,)] = len(quota_ok)
                stats["%s_quota_sync_fail" % (storage_name,)] = len(quota_fail)
                stats["%s_quota_sync_fail_warning" % (storage_name,)] = STORAGE_QUOTA_LIMIT_WARNING
                stats["%s_quota_sync_fail_critical" % (storage_name,)] = STORAGE_QUOTA_LIMIT_CRITICAL

        (vos_ok, vos_fail) = ([], [])
        if opts.options.vo:
            # FIXME: when api has changed, limit to modified per institute here
            changed_vos = client.vo.modified[last_timestamp].get()[1]
            changed_vo_quota = client.quota.vo.modified[last_timestamp].get()[1]

            vos = sorted(set([v['vsc_id'] for v in changed_vos] +
                             [v['virtual_organisation'] for v in changed_vo_quota]))

            logger.info("Found %d %s VOs that have changed in the accountpage since %s" %
                        (len(changed_vos), institute, last_timestamp))
            logger.info("Found %d %s VOs that have changed quota in the accountpage since %s" %
                        (len(changed_vo_quota), institute, last_timestamp))
            logger.debug("Found the following {institute} VOs: {vos}".format(institute=institute, vos=vos))

            for storage_name in opts.options.storage:
                (vos_ok, vos_fail) = process_vos(opts.options,
                                                 vos,
                                                 storage_name,
                                                 client,
                                                 last_timestamp,
                                                 institute)
                stats["%s_vos_sync" % (storage_name,)] = len(vos_ok)
                stats["%s_vos_sync_fail" % (storage_name,)] = len(vos_fail)
                stats["%s_vos_sync_fail_warning" % (storage_name,)] = STORAGE_VO_LIMIT_WARNING
                stats["%s_vos_sync_fail_critical" % (storage_name,)] = STORAGE_VO_LIMIT_CRITICAL

        if not (users_fail or quota_fail or vos_fail):
            (_, ldap_timestamp) = convert_timestamp(start_time)
            if not opts.options.dry_run:
                write_timestamp(SYNC_TIMESTAMP_FILENAME, ldap_timestamp)
    except Exception as err:
        logger.exception("critical exception caught: %s" % (err))
        opts.critical("Script failed in a horrible way")
        sys.exit(NAGIOS_EXIT_CRITICAL)

    opts.epilogue("%s users and VOs synchronised" % institute, stats)


if __name__ == '__main__':
    main()
