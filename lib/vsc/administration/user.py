# -*- coding: latin-1 -*-
#
# Copyright 2012-2018 Ghent University
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
This file contains the utilities for dealing with users on the VSC.

@author: Stijn De Weirdt (Ghent University)
@author: Andy Georges (Ghent University)
"""

import logging
import os

from urllib2 import HTTPError

from vsc.utils import fancylogger
from vsc.accountpage.wrappers import mkVscAccountPubkey, mkVscHomeOnScratch
from vsc.accountpage.wrappers import mkVscAccount, mkUserGroup
from vsc.accountpage.wrappers import mkGroup, mkVscUserSizeQuota
from vsc.administration.tools import create_stat_directory
from vsc.config.base import VSC, VscStorage, VSC_DATA, VSC_HOME, GENT_PRODUCTION_SCRATCH, GENT
from vsc.config.base import NEW, MODIFIED, MODIFY, ACTIVE
from vsc.filesystem.gpfs import GpfsOperations
from vsc.filesystem.posix import PosixOperations


# Cache for user instances
_users_cache = {
    'VscAccountPageUser': {},
    'VscTier2AccountpageUser': {},
}


log = fancylogger.getLogger(__name__)


class UserStatusUpdateError(Exception):
    pass


class VscAccountPageUser(object):
    """
    A user who gets his own information from the accountpage through the REST API.
    """

    def __init__(self, user_id, rest_client, account=None, pubkeys=None, use_user_cache=False):
        """
        Initialise.

        @param account: can be a VscAccount namedtuple, to avoid calling the REST api.
        @param pubkeys: can be a VscAccountPubkey namedtuple, to avoid calling the REST api.
        """
        self.user_id = user_id
        self.rest_client = rest_client

        # init global cache
        if use_user_cache:
            self._cache = _users_cache[self.__class__.__name__].setdefault(user_id, {})
        else:
            self._cache = {}

        if not self._cache:
            self._init_cache(pubkeys=pubkeys, account=account)

    def _init_cache(self, **kwargs):
        self._cache['pubkeys'] = kwargs.get('pubkeys', None)
        self._cache['account'] = kwargs.get('account', None)
        self._cache['usergroup'] = None
        self._cache['home_on_scratch'] = None

    @property
    def account(self):
        if not self._cache['account']:
            self._cache['account'] = mkVscAccount((self.rest_client.account[self.user_id].get())[1])
        return self._cache['account']

    @property
    def person(self):
        return self.account.person

    @property
    def usergroup(self):
        if not self._cache['usergroup']:
            if self.person.institute_login in ('x_admin', 'admin', 'voadmin'):
                # TODO to be removed when magic site admin usergroups are purged from code
                self._cache['usergroup'] = mkGroup((self.rest_client.group[self.user_id].get())[1])
            else:
                self._cache['usergroup'] = mkUserGroup((self.rest_client.account[self.user_id].usergroup.get()[1]))

        return self._cache['usergroup']

    @property
    def home_on_scratch(self):
        if self._cache['home_on_scratch'] is None:
            hos = self.rest_client.account[self.user_id].home_on_scratch.get()[1]
            self._cache['home_on_scratch'] = [mkVscHomeOnScratch(h) for h in hos]
        return self._cache['home_on_scratch']

    @property
    def pubkeys(self):
        if self._cache['pubkeys'] is None:  # an empty list is allowed :)
            ps = self.rest_client.account[self.user_id].pubkey.get()[1]
            self._cache['pubkeys'] = [mkVscAccountPubkey(p) for p in ps if not p['deleted']]
        return self._cache['pubkeys']

    def get_institute_prefix(self):
        """
        Get the first letter of the institute the user belongs to.
        """
        return self.person.institute['site'][0]


class VscTier2AccountpageUser(VscAccountPageUser):
    """
    A user on each of our Tier-2 system using the account page REST API
    to retrieve its information.
    """
    def __init__(self, user_id, storage=None, pickle_storage='VSC_SCRATCH_KYUKON', rest_client=None,
                 account=None, pubkeys=None, host_institute=None, use_user_cache=False):
        """
        Initialisation.
        @type vsc_user_id: string representing the user's VSC ID (vsc[0-9]{5})
        """
        super(VscTier2AccountpageUser, self).__init__(user_id, rest_client, account=account,
                                                      pubkeys=pubkeys, use_user_cache=use_user_cache)

        self.pickle_storage = pickle_storage
        if not storage:
            self.storage = VscStorage()
        else:
            self.storage = storage

        self.vsc = VSC()
        self.gpfs = GpfsOperations()  # Only used when needed
        self.posix = PosixOperations()
        self.host_institute = host_institute

    def _init_cache(self, **kwargs):
        super(VscTier2AccountpageUser, self)._init_cache(**kwargs)
        self._cache['quota'] = {}

    @property
    def user_home_quota(self):
        if not self._cache['quota']:
            self._init_quota_cache()
        return self._cache['quota']['home']

    @property
    def user_data_quota(self):
        if not self._cache['quota']:
            self._init_quota_cache()
        return self._cache['quota']['data']

    @property
    def user_scratch_quota(self):
        if not self._cache['quota']:
            self._init_quota_cache()
        return self._cache['quota']['scratch']

    @property
    def vo_data_quota(self):
        if not self._cache['quota']:
            self._init_quota_cache()
        return self._cache['quota']['vo']['data']

    @property
    def vo_scratch_quota(self):
        if not self._cache['quota']:
            self._init_quota_cache()
        return self._cache['quota']['vo']['scratch']

    def _init_quota_cache(self):
        if self.host_institute is None:
            logging.warn("_init_quota_cache with host_institute None")
        all_quota = [mkVscUserSizeQuota(q) for q in self.rest_client.account[self.user_id].quota.get()[1]]
        # we no longer set defaults, since we do not want to accidentally revert people to some default
        # that is lower than their actual quota if the accountpage goes down in between retrieving the users
        # and fetching the quota
        institute_quota = [q for q in all_quota if q.storage['institute'] == self.host_institute]
        fileset_name = self.vsc.user_grouping(self.account.vsc_id)  # FIXME

        def user_proposition(quota, storage_type):
            return quota.fileset == fileset_name and quota.storage['storage_type'] == storage_type

        # Non-UGent users who have quota in Gent, e.g., in a VO, should not have these set
        if self.person.institute['site'] == self.host_institute:
            self._cache['quota']['home'] = [q.hard for q in institute_quota if user_proposition(q, 'home')][0]
            self._cache['quota']['data'] = [q.hard for q in institute_quota
                                            if user_proposition(q, 'data')
                                            and not q.storage['name'].endswith('SHARED')][0]
            self._cache['quota']['scratch'] = filter(lambda q: user_proposition(q, 'scratch'), institute_quota)
        else:
            self._cache['quota']['home'] = None
            self._cache['quota']['data'] = None
            self._cache['quota']['scratch'] = None

        fileset_name = 'gvo'

        def user_vo_proposition(quota, storage_type):
            return quota.fileset.startswith(fileset_name) and quota.storage['storage_type'] == storage_type

        self._cache['quota']['vo'] = {}
        self._cache['quota']['vo']['data'] = [q for q in institute_quota if user_vo_proposition(q, 'data')]
        self._cache['quota']['vo']['scratch'] = [q for q in institute_quota if user_vo_proposition(q, 'scratch')]

    def pickle_path(self):
        """Provide the location where to store pickle files for this user.

        This location is the user'path on the pickle_storage specified when creating
        a VscTier2AccountpageUser instance.
        """
        (path, _) = self.storage.path_templates[GENT][self.pickle_storage]['user'](self.account.vsc_id)
        return os.path.join(self.storage[self.pickle_storage].gpfs_mount_point, path)

    def _create_grouping_fileset(self, filesystem_name, path, fileset_name):
        """Create a fileset for a group of 100 user accounts

        - creates the fileset if it does not already exist
        """
        self.gpfs.list_filesets()
        logging.info("Trying to create the grouping fileset %s with link path %s", fileset_name, path)

        if not self.gpfs.get_fileset_info(filesystem_name, fileset_name):
            logging.info("Creating new fileset on %s with name %s and path %s", filesystem_name, fileset_name, path)
            base_dir_hierarchy = os.path.dirname(path)
            self.gpfs.make_dir(base_dir_hierarchy)
            self.gpfs.make_fileset(path, fileset_name)
        else:
            logging.info("Fileset %s already exists for user group of %s ... not creating again.",
                         fileset_name, self.account.vsc_id)

        self.gpfs.chmod(0o755, path)

    def _get_mount_path(self, storage_name, mount_point):
        """Get the mount point for the location we're running"""
        if mount_point == "login":
            mount_path = self.storage[storage_name].login_mount_point
        elif mount_point == "gpfs":
            mount_path = self.storage[storage_name].gpfs_mount_point
        else:
            logging.error("mount_point (%s) is not login or gpfs", mount_point)
            raise Exception("mount_point (%s) is not designated as gpfs or login" % (mount_point,))

        return mount_path

    def _get_path(self, storage_name, mount_point="gpfs"):
        """Get the path for the (if any) user directory on the given storage_name."""
        (path, _) = self.storage.path_templates[GENT][storage_name]['user'](self.account.vsc_id)
        return os.path.join(self._get_mount_path(storage_name, mount_point), path)

    def _get_grouping_path(self, storage_name, mount_point="gpfs"):
        """Get the path and the fileset for the user group directory (and associated fileset)."""
        (path, fileset) = self.storage.path_templates[GENT][storage_name]['user'](self.account.vsc_id)
        return (os.path.join(self._get_mount_path(storage_name, mount_point), os.path.dirname(path)), fileset)

    def _home_path(self, mount_point="gpfs"):
        """Return the path to the home dir."""
        return self._get_path(VSC_HOME, mount_point)

    def _data_path(self, mount_point="gpfs"):
        """Return the path to the data dir."""
        return self._get_path(VSC_DATA, mount_point)

    def _scratch_path(self, storage_name, mount_point="gpfs"):
        """Return the path to the scratch dir"""
        return self._get_path(storage_name, mount_point)

    def _grouping_home_path(self, mount_point="gpfs"):
        """Return the path to the grouping fileset for the users on data."""
        return self._get_grouping_path(VSC_HOME, mount_point)

    def _grouping_data_path(self, mount_point="gpfs"):
        """Return the path to the grouping fileset for the users on data."""
        return self._get_grouping_path(VSC_DATA, mount_point)

    def _grouping_scratch_path(self, storage_name, mount_point="gpfs"):
        """Return the path to the grouping fileset for the users on the given scratch filesystem."""
        return self._get_grouping_path(storage_name, mount_point)

    def _create_user_dir(self, grouping_f, path_f, storage_name):
        """Create the directories and files for some user location.
        
        @type grouping: function that yields the grouping path for the location.
        @type path: function that yields the actual path for the location.
        """
        try:
            (grouping_path, fileset) = grouping_f()
            self._create_grouping_fileset(self.storage[storage_name].filesystem, grouping_path, fileset)

            path = path_f()
            if self.gpfs.is_symlink(path):
                logging.warning("Trying to make a user dir, but a symlink already exists at %s", path)
                return

            create_stat_directory(
                path,
                0o700,
                int(self.account.vsc_id_number),
                int(self.usergroup.vsc_id_number),
                self.gpfs
            )
        except Exception:
            logging.exception("Could not create home dir for user %s", self.account.vsc_id)
            raise

    def create_home_dir(self):
        """Create all required files in the (future) user's home directory."""
        self._create_user_dir(self._grouping_home_path, self._home_path, VSC_HOME)

    def create_data_dir(self):
        """Create the user's directory on the HPC data filesystem."""
        self._create_user_dir(self._grouping_data_path, self._data_path, VSC_DATA)

    def create_scratch_dir(self, storage_name):
        """Create the user's directory on the given scratch filesystem."""
        self._create_user_dir(
            lambda: self._grouping_data_path(storage_name),
            lambda: self._scratch_path(storage_name),
            storage_name)

    def _set_quota(self, storage_name, path, hard):
        """Set the given quota on the target path.

        @type path: path into a GPFS mount
        @type hard: hard limit
        """
        if not hard:
            logging.error("No user quota set for %s", storage_name)
            return

        quota = hard * 1024 * self.storage[storage_name].data_replication_factor
        soft = int(self.vsc.quota_soft_fraction * quota)

        logging.info("Setting quota for %s on %s to %d", storage_name, path, quota)

        # LDAP information is expressed in KiB, GPFS wants bytes.
        self.gpfs.set_user_quota(soft, int(self.account.vsc_id_number), path, quota)
        self.gpfs.set_user_grace(path, self.vsc.user_storage_grace_time)  # 7 days

    def set_home_quota(self):
        """Set USR quota on the home FS in the user fileset."""
        path = self._home_path()
        hard = self.user_home_quota
        self._set_quota(VSC_HOME, path, hard)

    def set_data_quota(self):
        """Set USR quota on the data FS in the user fileset."""
        (path, _) = self._grouping_data_path()
        hard = self.user_data_quota
        self._set_quota(VSC_DATA, path, hard)

    def set_scratch_quota(self, storage_name):
        """Set USR quota on the scratch FS in the user fileset."""
        quota = filter(lambda q: q.storage['name'] in (storage_name,), self.user_scratch_quota)
        if not quota:
            logging.error("No scratch quota information available for %s", storage_name)
            return

        if self.storage[storage_name].user_grouping_fileset:
            (path, _) = self._grouping_scratch_path(storage_name)
        else:
            # Hack; this should actually become the link path of the fileset
            # that contains the path (the file, not the followed symlink)
            path = os.path.normpath(os.path.join(self._scratch_path(storage_name), '..'))

        self._set_quota(storage_name, path, quota[0].hard)

    def populate_home_dir(self):
        """Store the required files in the user's home directory.

        Does not overwrite files that may contain user defined content.
        """
        path = self._home_path()
        self.gpfs.populate_home_dir(int(self.account.vsc_id_number),
                                    int(self.usergroup.vsc_id_number),
                                    path,
                                    [p.pubkey for p in self.pubkeys])

    def __setattr__(self, name, value):
        """Override the setting of an attribute:

        - dry_run: set this here and in the gpfs and posix instance fields.
        - otherwise, call super's __setattr__()
        """

        if name == 'dry_run':
            self.gpfs.dry_run = value
            self.posix.dry_run = value

        super(VscTier2AccountpageUser, self).__setattr__(name, value)



cluster_user_pickle_location_map = {
    'kyukon': VscTier2AccountpageUser,
}

cluster_user_pickle_store_map = {
    'kyukon': 'VSC_SCRATCH_KYUKON',
}


def update_user_status(user, client):
    """
    Change the status of the user's account in the account page to active.
    The usergroup status is always in sync with thte accounts status
    """
    if user.dry_run:
        log.info("User %s has account status %s. Dry-run, not changing anything", user.user_id, user.account.status)
        return

    if user.account.status not in (NEW, MODIFIED, MODIFY):
        log.info("Account %s has status %s, not changing" % (user.user_id, user.account.status))
        return

    payload = {"status": ACTIVE}
    try:
        response_account = client.account[user.user_id].patch(body=payload)
    except HTTPError as err:
        log.error("Account %s and UserGroup %s status were not changed", user.user_id, user.user_id)
        raise UserStatusUpdateError("Account %s status was not changed - received HTTP code %d" % err.code)
    else:
        account = mkVscAccount(response_account[1])
        if account.status == ACTIVE:
            log.info("Account %s status changed to %s" % (user.user_id, ACTIVE))
        else:
            log.error("Account %s status was not changed", user.user_id)
            raise UserStatusUpdateError("Account %s status was not changed, still at %s" %
                                        (user.user_id, account.status))


def process_users_quota(options, user_quota, storage_name, client, host_institute=None, use_user_cache=True):
    """
    Process the users' quota for the given storage.
    """
    error_quota = []
    ok_quota = []

    for quota in user_quota:
        user = VscTier2AccountpageUser(quota.user,
                                       rest_client=client,
                                       host_institute=host_institute,
                                       use_user_cache=use_user_cache)
        user.dry_run = options.dry_run

        try:
            if storage_name in ['VSC_HOME']:
                user.set_home_quota()

            if storage_name in ['VSC_DATA']:
                user.set_data_quota()

            if storage_name in GENT_PRODUCTION_SCRATCH:
                user.set_scratch_quota(storage_name)

            ok_quota.append(quota)
        except Exception:
            log.exception("Cannot process user %s" % (user.user_id))
            error_quota.append(quota)

    return (ok_quota, error_quota)


def process_users(options, account_ids, storage_name, client, host_institute=None, use_user_cache=True):
    """
    Process the users.

    We make a distinction here between three types of filesystems.
        - home (unique)
            - create and populate the home directory
        - data (unique)
            - create the grouping fileset if needed
            - create the user data directory
        - scratch (multiple)
            - create the grouping fileset if needed
            - create the user scratch directory

    """
    error_users = []
    ok_users = []

    for vsc_id in sorted(account_ids):
        user = VscTier2AccountpageUser(vsc_id,
                                       rest_client=client,
                                       host_institute=host_institute,
                                       use_user_cache=use_user_cache)
        user.dry_run = options.dry_run

        try:
            if storage_name in ['VSC_HOME']:
                user.create_home_dir()
                user.populate_home_dir()
                update_user_status(user, client)

            if storage_name in ['VSC_DATA']:
                user.create_data_dir()

            if storage_name in GENT_PRODUCTION_SCRATCH:
                user.create_scratch_dir(storage_name)

            ok_users.append(user)
        except Exception:
            log.exception("Cannot process user %s" % (user.user_id))
            error_users.append(user)

    return (ok_users, error_users)
