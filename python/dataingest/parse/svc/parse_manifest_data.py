#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import os
import pprint

from base import BaseObject
from base import MandatoryParamError
from datamongo import BaseMongoClient
from datamongo import CendantCollection
from datamongo import CreateTextIndex
from datamongo import CreateFieldIndex
from nlutext import TextParser


class ParseManifestData(BaseObject):
    """ given a parse manifest:
        1.  take data from a source mongo collection
        2.  parse it
        3.  and write it to a target mongo collection """

    def __init__(self,
                 some_manifest_name: str,
                 some_activity_name: str,
                 first: int = -1,
                 last: int = -1,
                 is_debug: bool = False):
        """
        Created:
            12-Mar-2019
            craig.trim@ibm.com
        Updated
            3-Apr-2019
            craig.trim@ibm.com
            *   one sentence per field
        Updated:
            4-Apr-2019
            craig.trim@ibm.com
            *   incremental parse capability
        Updated:
            24-Jun-2019
            craig.trim@ibm.com
            *   added text index
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/371
        Updated:
            2-Aug-2019
            craig.trim@ibm.com
            *   added mongo connection as parameter to manifest connectors
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/588
        Updated:
            2-Sep-2019
            craig.trim@ibm.com
            *   added incremental-retrieval and documentation to source-records
        Updated:
            17-Dec-2019
            xavier.verges@es.ibm.com
            *   removed incremental-retrieval and documentation to source-records
            *   adapted to kubernetes and RQ
        :param some_manifest_name:
            the name of the manifest
        :param some_activity_name:
            the name of the activity within the manifest
        """
        from dataingest.core.dmo import ManifestActivityFinder
        from dataingest.core.dmo import ManifestConnectorForMongo
        BaseObject.__init__(self, __name__)
        if not some_manifest_name:
            raise MandatoryParamError("Manifest Name")
        if not some_activity_name:
            raise MandatoryParamError("Activity Name")

        self.is_debug = is_debug
        self.text_parser = TextParser(is_debug=is_debug)

        self.manifest_name = some_manifest_name
        self.activity_name = some_activity_name
        self.first = first
        self.last = last
        self._mongo_client = BaseMongoClient()
        self._manifest_data = ManifestActivityFinder(self.manifest_name,
                                                     self.activity_name).process()
        self._manifest_connector_class = ManifestConnectorForMongo

        if self.is_debug:
            self.logger.debug("Instantiate ParseManifestData")

    def _target_collection(self) -> CendantCollection:

        if self.is_debug:
            self.logger.debug("\n".join([
                "Loading Manifest Target Connector",
                f"{pprint.pformat(self._manifest_data['target'], indent=4)}"]))

        connector = self._manifest_connector_class(some_base_client=self._mongo_client,
                                                   some_manifest_entry=self._manifest_data["target"],
                                                   is_debug=self.is_debug)
        return connector.process()

    def _source_records(self) -> list:

        from dataingest.parse.dmo import ParseDataExtractor

        data_extractor = ParseDataExtractor(self._manifest_data,
                                            mongo_client=self._mongo_client,
                                            is_debug=self.is_debug)

        some_source_records = data_extractor.process(start_record=self.first,
                                                     end_record=self.last)

        self.logger.debug(f"Returning Source Records "
                          f"(type=2, total={len(some_source_records)})")
        return some_source_records

    def _parse_records(self,
                       target_collection: CendantCollection,
                       source_records: list) -> int:
        from dataingest.parse.svc import ParseRecordsFromMongo

        parser = ParseRecordsFromMongo(target_collection=target_collection,
                                       source_records=source_records,
                                       is_debug=self.is_debug)

        return parser.process(threshold=5)

    def source_collections(self) -> list:
        collection = self._manifest_connector_class(self._manifest_data['source'],
                                                    some_base_client=self._mongo_client,
                                                    is_debug=self.is_debug).process()
        return [(collection.collection_name, collection.count())]

    def flush_target(self) -> None:
        target_collection = self._target_collection()
        target_collection.delete(keep_indexes=False)

    def index_target(self) -> None:
        index_options = {
            'background': True
        }
        self.logger.debug("Creating text indexes...")
        target_collection = self._target_collection()

        field_indexer = CreateFieldIndex(is_debug=self.is_debug,
                                         collection=target_collection.collection)
        text_indexer = CreateTextIndex(is_debug=self.is_debug,
                                       collection=target_collection.collection)

        text_indexer.process(field_name='fields.normalized',
                             index_options=index_options)

        field_indexer.process(field_name='div_field',
                              index_options=index_options)
        field_indexer.process(field_name='key_field',
                              index_options=index_options)

    def process(self) -> int:
        """
        :param start_record:
        :param end_record:
        :param flush_records:
            if true     wipe target mongo collection
            if false    append to mongo
                        note: no duplicate checking occurs
        :return:
        """

        target_collection = self._target_collection()
        source_records = self._source_records()

        if not source_records or not len(source_records):
            self.logger.warn("\n".join([
                "No Source Records Found",
                f"\tSource: {self._manifest_data['source']}, "
                f"{os.environ[self._manifest_data['source']['collection'][1:]]}",
                f"\tTarget: {self._manifest_data['target']}, "
                f"{os.environ[self._manifest_data['target']['collection'][1:]]}",
                f"\tStart Record: {self.first}",
                f"\tEnd Record: {self.last}"]))

            return 0

        handled_records = self._parse_records(target_collection,
                                              source_records)

        if self.is_debug:
            self.logger.debug("\n".join([
                "Manifest Parsing Complete",
                f"\tURL: {target_collection.base_client.url}",
                f"\tCollection Name: {target_collection.collection_name}",
                f"\tTotal Records: {handled_records}"]))

        return handled_records
