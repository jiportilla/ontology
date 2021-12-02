#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from pandas import DataFrame

from base import BaseObject
from base import RecordUnavailableRecord
from dataingest import ManifestActivityFinder
from datamongo import BaseMongoClient


class FindDimensionRecordsByKey(BaseObject):

    def __init__(self,
                 mongo_client: BaseMongoClient = None,
                 is_debug: bool = False):
        """
        Created:
            13-May-2019
            craig.trim@ibm.com
            *   refactored out of 'dimensions-api'
        Updated:
            29-Oct-2019
            craig.trim@ibm.com
            *   remove 'entity-schema' in favor of new approach
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/916#issuecomment-15620801
        """
        BaseObject.__init__(self, __name__)
        if not mongo_client:
            mongo_client = BaseMongoClient()

        self._is_debug = is_debug
        self._mongo_client = mongo_client

    def process(self,
                xdm_schema: str,
                source_name: str,
                collection_name_tag: str,
                collection_name_xdm: str,
                key_field: str = None) -> DataFrame or None:
        """
        Purpose:
            Return dimension records by key-field
        :param source_name:
            the dimension manifest source name
        :param collection_name_tag:
            the tag collection name to search
        :param collection_name_xdm:
            the xdm collection name to search
        :param xdm_schema:
            the name of the schema to perform the type lookup
            Notes:
            -   typically either 'supply' or 'learning'
            -   the full list is on path 'resources/config/dimensionality'
        :param key_field:
            the key field to search by
            e.g., Open Seat ID, Serial Number, Learning Activity ID
        :return:
        """
        from cendantdim.batch.svc import ProcessSingleRecord

        d_manifest = ManifestActivityFinder(some_manifest_name="dimension-manifest",
                                            some_manifest_activity=source_name).process()

        psr = ProcessSingleRecord(xdm_schema=xdm_schema,
                                  d_manifest=d_manifest,
                                  is_debug=self._is_debug,
                                  mongo_client=self._mongo_client,
                                  collection_name_tag=collection_name_tag,
                                  collection_name_xdm=collection_name_xdm)

        if not key_field:
            key_field = "random"

        def find_record() -> DataFrame or None:
            try:
                df = psr.process(key_field=key_field)
                if df.empty:
                    self.logger.warning(f"Empty DataFrame ("
                                        f"key-field={key_field})")
                    return None
                return df

            except RecordUnavailableRecord:
                self.logger.warning(f"Record Not Found ("
                                    f"key-field={key_field})")
                return None

        return find_record()
