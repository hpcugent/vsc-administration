#
# Copyright 2020-2020 Ghent University
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
Tests for the postfix email address sync
"""


from collections import namedtuple
from vsc.install.testing import TestCase

CANONICAL_DB = "\n".join([
    "blup001@vscentrum.be zaphob.beeblebrox@betelgeuse.uni",
    "blup002@vscentrum.be ford.perfect@earth.uni"
])


Account = namedtuple("Account", ["vsc_id", "email"])


ACTIVE_ACCOUNTS = [
    Account(vsc_id="blup002", email="ford.perfect@earth.uni"),
    Account(vsc_id="blup003", email="tricia.macmillan@earth.uni"),
]

INACTIVE_ACCOUNTS = [
    Account(vsc_id="blup001", email="zaphob.beeblebrox@betelgeuse.uni")
]


class PostfixSyncTest(TestCase):
    """
    Test the sync.
    """

    @mock.patch(vsc.administration.postfix.Sync)
    def test_sync(self, mock_sync):
        open_name = '%s.open' % __name__
        canonical_file = MagicMock(spec=file)
        with patch(open_name, create=True) as mock_open:
            mock_open.return_value = canonical_file
            canonical_file.read_lines.return_value = CANONICAL_DB

            mock_sync.get_accounts.return_value = (ACTIVE_ACCOUNTS, INACTIVE_ACCOUNTS)

            canonical_file.write.assert_called_with(
                "\n".join([
                    "blup002@vscentrum.be ford.perfect@earth.uni"
                    "blup003@vscentrum.be tricia.macmillan@earth.uni",
                ])
            )
