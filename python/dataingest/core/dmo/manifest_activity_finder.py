#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import os

from base import BaseObject
from base import FileIO
from base import MandatoryParamError


class ManifestActivityFinder(BaseObject):
    """ find the correct ingest manifest by name """

    def __init__(self,
                 some_manifest_name: str,
                 some_manifest_activity: str):
        """
        Created:
            11-Mar-2019
            craig.trim@ibm.com
        Updated:
            2-May-2019
            craig.trim@ibm.com
            *   more robust matching of manifest name and activity
        :param some_manifest_name:
            the actual name of the manifest file
        :param some_manifest_activity:
            the name of the activity within the manifest
        """
        BaseObject.__init__(self, __name__)
        if not some_manifest_name:
            raise MandatoryParamError("Manifest Name")
        if not some_manifest_activity:
            raise MandatoryParamError("Activity Name")

        self.manifest_name = some_manifest_name.lower().strip()
        self.manifest_activity = some_manifest_activity.lower().strip()

    def _manifest(self) -> dict:
        path = os.path.join(os.environ["CODE_BASE"],
                            "resources/manifest",
                            "{}.yml".format(self.manifest_name))
        return FileIO.file_to_yaml(path)

    def process(self) -> dict:
        manifest = self._manifest()

        def _has_match(x: dict) -> bool:
            return x["name"].lower().strip() == self.manifest_activity

        result = [x for x in manifest["activity"]
                  if _has_match(x)]

        if not result or not len(result):
            names = [x["name"].lower().strip() for x in manifest["activity"]]
            raise NotImplementedError("\n".join([
                "Manifest Activity Not Found",
                f"\tActivity Name: '{self.manifest_activity}' not in {names}"]))

        if len(result) > 1:
            raise ValueError("\n".join([
                "Multiple Manifest Activities Found",
                "\tActivity Name: {}".format(self.manifest_activity),
                "\tTotal Found: {}".format(len(result))]))

        return result[0]
