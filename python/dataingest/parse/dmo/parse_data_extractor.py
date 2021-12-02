#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from base import MandatoryParamError
from datamongo import BaseMongoClient
from datamongo import CendantCollection


class ParseDataExtractor(BaseObject):
    """ """

    def __init__(self,
                 some_manifest: dict,
                 mongo_client: BaseMongoClient,
                 is_debug: bool = True):
        """
        Created:
            12-Mar-2019
            craig.trim@ibm.com
        Updated:
            2-Aug-2019
            craig.trim@ibm.com
            *   added mongo client as a parameter
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/588
        Updated:
            17-Dec-2019
            xavier.verges@es.ibm.com
            *   removed unused code
            *   tweaked skip and limit params. E.g. first=4 and last=5, means skip=4 and limit=5-4+1
        :param some_manifest:
            the parse manifest
        """
        BaseObject.__init__(self, __name__)
        if not some_manifest:
            raise MandatoryParamError("Manifest")

        self._is_debug = is_debug
        self._manifest = some_manifest
        self._mongo_client = mongo_client

    def _source_collection(self) -> CendantCollection:
        """
        :return:
            a connection to the MongoDB collection with source data
        """
        from dataingest.core.dmo import ManifestConnectorForMongo

        if self._is_debug:
            self.logger.debug("Loading Manifest Source Connector")

        connector = ManifestConnectorForMongo(some_base_client=self._mongo_client,
                                              some_manifest_entry=self._manifest["source"],
                                              is_debug=True)
        return connector.process()

    def process(self,
                start_record: int,
                end_record: int) -> list:
        """
        :param start_record:
        :param end_record:
        :return:
            source records
        """

        source_collection = self._source_collection()
        source_records = source_collection.skip_and_limit(start_record,
                                                          (end_record - start_record + 1))

        self.logger.info("\n".join([
            "Loaded Source Records",
            f"\tTotal: {len(source_records)}",
            f"\tName: {source_collection.collection.name}",
            f"\tStart Record: {start_record}",
            f"\tEnd Record: {end_record}",
            f"\tTotal Records: {len(source_records)}"]))

        return source_records
