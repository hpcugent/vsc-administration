#
# Copyright 2015-2022 Ghent University
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
Tests for vsc.administration.slurm.*

@author: Andy Georges (Ghent University)
"""

from vsc.install.testing import TestCase

from vsc.administration.slurm.sacct import parse_slurm_sacct_dump, SlurmJobs, SacctTypes, SacctParseException


class SlurmSacctmgrTest(TestCase):
    def test_parse_slurmm_sacct_dump(self):
        """Test that the sacct output is correctly processed."""

        sacct_active_jobs_output = [
            "JobID|JobName|Partition|Account|AllocCPUS|State|ExitCode",
            "14367800|normal|part1|acc1|1|RUNNING|0:0",
            "14367800.batch|batch||acc1|1|RUNNING|0:0",
            "14367800.extern|extern||acc1|1|RUNNING|0:0",
        ]
        info = parse_slurm_sacct_dump(sacct_active_jobs_output, SacctTypes.jobs)
        self.assertEqual(set(info), set([
            SlurmJobs(JobID='14367800', JobName='normal', Partition='part1', Account='acc1', AllocCPUS='1', State='RUNNING', ExitCode='0:0'),
            SlurmJobs(JobID='14367800.batch', JobName='batch', Partition='', Account='acc1', AllocCPUS='1', State='RUNNING', ExitCode='0:0'),
            SlurmJobs(JobID='14367800.extern', JobName='extern', Partition='', Account='acc1', AllocCPUS='1', State='RUNNING', ExitCode='0:0')
        ]))


        with self.assertRaises(SacctParseException):
            parse_slurm_sacct_dump("sacct_active_jobs_output", "doesnotexist")
