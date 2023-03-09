#
# Copyright 2013-2022 Ghent University
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
sacct commands
"""
import logging
import re
from enum import Enum

from vsc.accountpage.wrappers import mkNamedTupleInstance
from vsc.config.base import ANTWERPEN, BRUSSEL, GENT, LEUVEN
from vsc.utils.missing import namedtuple_with_defaults
from vsc.utils.run import asyncloop

SLURM_SACCT = "/usr/bin/sacct"

SLURM_ORGANISATIONS = {
    ANTWERPEN: 'uantwerpen',
    BRUSSEL: 'vub',
    GENT: 'ugent',
    LEUVEN: 'kuleuven',
}


class SacctParseException(Exception):
    pass

class SacctException(Exception):
    pass


class SacctTypes(Enum):
    jobs = "jobs"


# Fields for Slurm 20.11.
# FIXME: at some point this should be versioned

SacctJobsFields = [
    "JobID", "JobName", "Partition", "Account", "AllocCPUS", "State", "ExitCode",
]

SlurmJobs = namedtuple_with_defaults('SlurmJobs', SacctJobsFields)

def mkSlurmJobs(fields):
    """Make a named tuple from the given fields"""
    activejobs = mkNamedTupleInstance(fields, SlurmJobs)
    return activejobs

def parse_slurm_sacct_line(header, line, info_type):
    """Parse the line into the correct data type."""
    fields = line.split("|")

    if info_type == SacctTypes.jobs:
        creator = mkSlurmJobs
    else:
        raise SacctParseException("info_type %s does not exist.", info_type)

    return creator(dict(zip(header, fields)))


def parse_slurm_sacct_dump(lines, info_type):
    """Parse the sacctmgr dump from the listing."""
    acct_info = set()

    header = [w.replace(' ', '_').replace('%', 'PCT_') for w in lines[0].rstrip().split("|")]

    for line in lines[1:]:
        logging.debug("line %s", line)
        line = line.rstrip()
        try:
            info = parse_slurm_sacct_line(header, line, info_type)
        except Exception as err:
            logging.exception("Slurm sacct parse dump: could not process line %s [%s]", line, err)
            raise
        # This fails when we get e.g., the users and look at the account lines.
        # We should them just skip that line instead of raising an exception
        if info:
            acct_info.add(info)

    return acct_info


def get_slurm_sacct_active_jobs_for_user(user):
    """
    Get running and queued jobs for user.
    """
    (exitcode, contents) = asyncloop([
        SLURM_SACCT, "--allclusters", "--parsable2", "--state", "RUNNING,PENDING", "--user", user])
    if exitcode != 0:
        if re.search("sacct: error: Invalid user id: %s" % user, contents):
            logging.warning("User %s does not exist, assuming no active jobs.", user)
            return None
        else:
            raise SacctException("Cannot run sacct")

    info = parse_slurm_sacct_dump(contents.splitlines(), SacctJobsFields)
    return info
