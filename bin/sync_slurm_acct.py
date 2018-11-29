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
This script synchronises the users and VO's from the HPC account page to the Slurm database.

The script must result in an idempotent execution, to ensure nothing breaks.
"""

import logging
import sys

from vsc.accountpage.client import AccountpageClient
from vsc.accountpage.wrappers import mkVo
from vsc.administration.slurm.sync import get_slurm_acct_info, SyncTypes, SacctMgrException
from vsc.administration.slurm.sync import slurm_institute_accounts, slurm_vo_accounts, slurm_user_accounts
from vsc.config.base import GENT_SLURM_COMPUTE_CLUSTERS, GENT_PRODUCTION_COMPUTE_CLUSTERS
from vsc.utils import fancylogger
from vsc.utils.nagios import NAGIOS_EXIT_CRITICAL
from vsc.utils.run import RunQA, RunQAStdout
from vsc.utils.script_tools import ExtendedSimpleOption

logger = fancylogger.getLogger()
fancylogger.logToScreen(True)
fancylogger.setLogLevelInfo()

RunQA.LOOP_MAX_MISS_COUNT = 30
RunQAStdout.LOOP_MAX_MISS_COUNT = 30


NAGIOS_HEADER = "sync_slurm_acct"
NAGIOS_CHECK_INTERVAL_THRESHOLD = 60 * 60  # 60 minutes

SYNC_TIMESTAMP_FILENAME = "/var/cache/%s.timestamp" % (NAGIOS_HEADER)
SYNC_SLURM_ACCT_LOGFILE = "/var/log/%s.log" % (NAGIOS_HEADER)


def execute_commands(commands):
    """Run the specified commands"""

    for command in commands:
        logging.info("Running command: %s", command)

        # if one fails, we simply fail the script and should get notified
        (ec, _) = RunQA.run(command, qa={"(N/y):": "y"}, add_newline=False)
        if ec != 0:
            raise SacctMgrException("Command failed: {0}".format(command))


def main():
    """
    Main script. The usual.
    """

    options = {
        "nagios-check-interval-threshold": NAGIOS_CHECK_INTERVAL_THRESHOLD,
        "access_token": ("OAuth2 token to access the account page REST API", None, "store", None),
        "account_page_url": (
            "URL of the account page where we can find the REST API",
            str,
            "store",
            "https://apivsc.ugent.be/django",
        ),
        "clusters": (
            "Cluster(s) (comma-separated) to sync for. "
            "Overrides GENT_SLURM_COMPUTE_CLUSTERS that are in production.",
            str,
            "store",
            None,
        ),
    }

    opts = ExtendedSimpleOption(options)
    stats = {}

    try:
        client = AccountpageClient(token=opts.options.access_token, url=opts.options.account_page_url + "/api/")

        last_timestamp = "201804010000Z"  # the beginning of time

        logging.info("Last recorded timestamp was %s" % (last_timestamp))

        slurm_account_info = get_slurm_acct_info(SyncTypes.accounts)
        slurm_user_info = get_slurm_acct_info(SyncTypes.users)

        logging.debug("%d accounts found", len(slurm_account_info))
        logging.debug("%d users found", len(slurm_user_info))

        if opts.options.clusters is not None:
            clusters = opts.options.clusters.split(",")
        else:
            clusters = [c for c in GENT_SLURM_COMPUTE_CLUSTERS if c in GENT_PRODUCTION_COMPUTE_CLUSTERS]

        sacctmgr_commands = []

        # make sure the institutes and the default accounts (VOs) are there for each cluster
        sacctmgr_commands += slurm_institute_accounts(slurm_account_info, clusters)

        # All users belong to a VO, so fetching the VOs is necessary/
        account_page_vos = [mkVo(v) for v in client.vo.get()[1]]

        # The VOs do not track active state of users, so we need to fetch all accounts as well
        active_accounts = set([a["vsc_id"] for a in client.account.get()[1] if a["isactive"]])

        # dictionary mapping the VO vsc_id on a tuple with the VO members and the VO itself
        account_page_members = dict([(vo.vsc_id, (set(vo.members), vo)) for vo in account_page_vos])

        # process all regular VOs
        sacctmgr_commands += slurm_vo_accounts(account_page_vos, slurm_account_info, clusters)

        # process VO members
        sacctmgr_commands += slurm_user_accounts(
            account_page_members,
            active_accounts,
            slurm_user_info,
            clusters,
            opts.options.dry_run
        )

        logging.info("Executing %d commands", len(sacctmgr_commands))

        if opts.options.dry_run:
            print("Commands to be executed:\n")
            print("\n".join([" ".join(c) for c in sacctmgr_commands]))
        else:
            execute_commands(sacctmgr_commands)

    except Exception as err:
        logger.exception("critical exception caught: %s" % (err))
        opts.critical("Script failed in a horrible way")
        sys.exit(NAGIOS_EXIT_CRITICAL)

    if not opts.options.dry_run:
        opts.epilogue("Accounts synced to slurm", stats)
    else:
        logger.info("Dry run done")


if __name__ == "__main__":
    main()
