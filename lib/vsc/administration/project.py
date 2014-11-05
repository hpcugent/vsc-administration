#!/usr/bin/env python
# -*- coding: latin-1 -*-
##
# Copyright 2012-2013 Ghent University
#
# This file is part of vsc-administration,
# originally created by the HPC team of Ghent University (http://ugent.be/hpc/en),
# with support of Ghent University (http://ugent.be/hpc),
# the Flemish Supercomputer Centre (VSC) (https://vscentrum.be/nl/en),
# the Hercules foundation (http://www.herculesstichting.be/in_English)
# and the Department of Economy, Science and Innovation (EWI) (http://www.ewi-vlaanderen.be/en).
#
# All rights reserved.
#
##
"""
This file provides utilities to set up projects on the VSC clusters.

@author: Andy Georges (Ghent University)
"""
import logging
import os
import pwd

from urllib2 import HTTPError

from vsc.accountpage.wrappers import VscAutogroup, VscGroup, VscProjectQuota
from vsc.config.base import Muk, VscStorage
from vsc.filesystem.ext import ExtOperations
from vsc.filesystem.gpfs import GpfsOperations
from vsc.filesystem.posix import PosixOperations


class VscProject(object):
    def __init__(self, project_id, rest_client):
        """
        Initialise.
        """
        self.project_id = project_id
        self.rest_client = rest_client

        # We immediately retrieve this information
        try:
            self.project = VscProject(**(rest_client.project[project_id].get()[1]))
        except HTTPError:
            logging.error("Cannot get information from the account page")
            raise

        self.group = VscAutogroup(**(self.project.group))


class MukProject(VscProject):
    """Project that will be run on Muk."""

    def __init__(self, project_id, storage=None, rest_client=None):
        """Initialisation.

        @type project_id: string
        @param project_id: the unique ID of the project, i.e.,  the LDAP cn entry
        """
        super(MukProject, self).__init__(project_id, rest_client)

        self.muk = Muk()

        self.ext = ExtOperations()
        self.gpfs = GpfsOperations()
        self.posix = PosixOperations()

        if not storage:
            self.storage = VscStorage()
        else:
            self.storage = storage

        self.scratch = self.gpfs.get_filesystem_info(self.muk.scratch_name)

        try:
            all_quota = [VscProjectQuota(**q) for q in rest_client.project[self.project_id].quota.get()[1]]
        except HTTPError:
            logging.exception("Unable to retrieve quota information from the accountpage")
            self.user_scratch_storage = 0
        else:
            muk_quota = filter(lambda q: q.storage['name'] == self.muk.storage_name, all_quota)
            if muk_quota:
                self.project_scratch_quota = muk_quota[0]['hard']
            else:
                self.project_scratch_quota = 250 * 1024 * 1024 * 1024

    def _scratch_path(self, mount_point="gpfs"):
        """Determines the path (relative to the scratch mount point)

        For a project with ID projectXYZUV this becomes projects/projectXYZ/projectYZUV.

        @returns: string representing the relative path for this project.
        """
        template = self.storage.path_templates[self.muk.storage_name]['project']
        if mount_point == "login":
            mount_path = self.storage[self.muk.storage_name].login_mount_point
        elif mount_point == "gpfs":
            mount_path = self.storage[self.muk.storage_name].gpfs_mount_point
        else:
            logging.error("mount_point (%s) is not login or gpfs" % (mount_point))
            raise Exception("wrong mount point type")

        return os.path.join(mount_path, template[0], template[1](self.project_id))

    def create_scratch_fileset(self):
        """Create a fileset for the VO on the data filesystem.

        - creates the fileset if it does not already exist
        - sets the (fixed) quota on this fileset for the VO
        """
        self.gpfs.list_filesets()
        fileset_name = self.project_id
        filesystem_name = self.muk.scratch_name
        path = self._scratch_path()

        if not self.gpfs.get_fileset_info(filesystem_name, fileset_name):
            logging.info("Creating new fileset for project %s on %s with name %s and path %s" % (self.project_id,
                                                                                                  filesystem_name,
                                                                                                  fileset_name,
                                                                                                  path))
            base_dir_hierarchy = os.path.dirname(path)
            self.gpfs.make_dir(base_dir_hierarchy)
            self.gpfs.make_fileset(path, fileset_name)
        else:
            logging.info("Fileset %s already exists for project %s ... not creating again." % (fileset_name,
                                                                                                self.project_id))
        self.gpfs.chmod(0770, path)

        if self.project.submitter:
            self.gpfs.chown(self.project.submitter.vsc_id_number, self.group.vsc_id_number, path)
        else:
            self.gpfs.chown(pwd.getpwnam('nobody').pw_uid, self.group.vsc_id_number, path)

        self.gpfs.set_fileset_quota(self.project_scratch_quota, path, fileset_name)

    def __setattr__(self, name, value):
        """Override the setting of an attribute:

        - dry_run: set this here and in the gpfs and posix instance fields.
        - othwerwise, call super's __setattr__()
        """

        if name == 'dry_run':
            self.gpfs.dry_run = value
            self.posix.dry_run = value

        super(MukProject, self).__setattr__(name, value)
