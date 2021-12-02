#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from base import RecordUnavailableRecord
from datamongo import BaseMongoClient
from datamongo import CendantCollection


class SingleRecordLocator(BaseObject):
    """ locate a single source record """

    def __init__(self,
                 collection_name: str,
                 base_mongo_client: BaseMongoClient = None,
                 is_debug: bool = False):
        """
        Created:
            15-May-2019
            craig.trim@ibm.com
            *   refactored out of process-single-record
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/243
        Updated:
            16-Jul-2019
            craig.trim@ibm.com
            *   use 'manifest-connnector-for-mongo' to access collections via env vars
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/450
        Updated:
            15-Oct-2019
            craig.trim@ibm.com
            *   don't pass the manifest in; use explicit source-collection-name instead
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1116#issuecomment-15308894
        Updated:
            15-Nov-2019
            craig.trim@ibm.com
            *   modify to return a single dict instead of a list
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1331#issuecomment-16030068
        """
        BaseObject.__init__(self, __name__)
        if not base_mongo_client:
            base_mongo_client = BaseMongoClient()

        self._is_debug = is_debug
        self._collection_name = collection_name
        self._base_mongo_client = base_mongo_client

        if self._is_debug:
            self.logger.debug("Instantiated SingleRecordLocator")

    def _source_collection(self) -> CendantCollection:
        return CendantCollection(is_debug=self._is_debug,
                                 some_base_client=self._base_mongo_client,
                                 some_collection_name=self._collection_name)

    def process(self,
                key_field: str) -> dict:
        """
        Purpose:
            Retrieve a Single Record
        :param key_field:
            a key field to search on
            e.g., Serial Number
        :return:
            dict        a single result
        """
        collection = self._source_collection()

        def _record() -> dict:  # GIT-1331-16030068
            if key_field.lower() == "random":
                return collection.random(total_records=1)[0]
            return collection.by_key_field(key_field)

        record = _record()

        if not record:
            raise RecordUnavailableRecord(f"Record Not Found "
                                          f"collection={collection.collection_name}, "
                                          f"key-field={key_field})")

        return record
