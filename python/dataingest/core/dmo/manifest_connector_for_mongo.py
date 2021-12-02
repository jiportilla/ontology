#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import os

from base import BaseObject
from base import MandatoryParamError
from datamongo import BaseMongoClient
from datamongo import CendantCollection


class ManifestConnectorForMongo(BaseObject):
    """ open the correct mongoDB connection for the target data source
        based on the manifest definition """

    def __init__(self,
                 some_manifest_entry: dict,
                 some_base_client: BaseMongoClient = None,
                 is_debug: bool = False):
        """
        Created:
            11-Mar-2019
            craig.trim@ibm.com
        Updated:
            14-Mar-2019
            craig.trim@ibm.com
            *   renamed from 'ingest-mongo-connection'
        Updated:
            16-Jul-2019
            craig.trim@ibm.com
            *   add is-debug parameter
            *   perform lookup if the collection-name is an env var
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/450
        :param some_manifest_entry:
            the name of the ingestion activity
        """
        BaseObject.__init__(self, __name__)
        if not some_manifest_entry:
            raise MandatoryParamError("Manifest Entry")
        if not some_base_client:
            some_base_client = BaseMongoClient()

        self.is_debug = is_debug
        self.base_client = some_base_client
        self.manifest_entry = some_manifest_entry

    def _manifest_entry(self) -> dict:
        """ validate the manifest entry by connection type
            only MongoDB is (currently) supported
        :return:
            the (validated) manifest entry
        """
        whitelist = ["mongodb", "mongo"]

        if self.manifest_entry["type"].lower() not in whitelist:
            raise NotImplementedError('\n'.join([
                "Connection Type Not Supported",
                "\tExpected: MongoDB",
                f"\tActual: {self.manifest_entry['type']}"]))

        return self.manifest_entry

    def collection_name(self) -> str:
        entry = self._manifest_entry()

        collection_name = entry["collection"]
        if collection_name.startswith("$"):
            collection_name = os.environ[collection_name[1:]]
        return collection_name

    def process(self) -> CendantCollection:
        entry = self._manifest_entry()

        return CendantCollection(is_debug=self.is_debug,
                                 some_base_client=self.base_client,
                                 some_db_name=entry["database"],
                                 some_collection_name=self.collection_name())
