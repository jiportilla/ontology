#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import os

from base import BaseObject
from base import MandatoryParamError


class RunManifestDataCommand(BaseObject):
    """Runs commands specified by a manifest activity"""

    def __init__(self,
                 some_manifest_name: str,
                 some_activity_name: str,
                 is_debug: bool = False):
        from dataingest.core.dmo import ManifestActivityFinder

        BaseObject.__init__(self, __name__)
        if not some_manifest_name:
            raise MandatoryParamError("Manifest Name")
        if not some_activity_name:
            raise MandatoryParamError("Activity Name")

        self.is_debug = is_debug
        self.command = ManifestActivityFinder(some_manifest_name,
                                              some_activity_name).process()['command']

    def process(self) -> int:
        return os.system(self.command)


def run_manifest_command(manifest_name: str,
                         activity_name: str):
    runner = RunManifestDataCommand(manifest_name, activity_name)
    command = runner.command
    rc = runner.process()
    if rc:
        raise Exception(f"Error {rc} executing '{command}'")
