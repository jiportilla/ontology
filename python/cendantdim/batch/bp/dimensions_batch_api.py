#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from datamongo import BaseMongoClient


class DimensionsBatchAPI(BaseObject):
    """
    """

    def __init__(self,
                 is_debug: bool = False):
        """
        Created:
            20-Aug-2019
            craig.trim@ibm.com
            *   refactored out of dimensions-batch-caller
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/785
        Updated:
            15-Oct-2019
            craig.trim@ibm.com
            *   update parameter list
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1123
        Updated:
            29-Oct-2019
            craig.trim@ibm.com
            *   remove 'entity-schema-finder' in favor of new approach
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/916#issuecomment-15620801
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug

    def multiple_records(self,
                         d_manifest: dict,
                         xdm_schema: str,
                         collection_name_tag: str,
                         collection_name_xdm: str,
                         start_record=None,
                         end_record=None,
                         flush_records: bool = False):
        from cendantdim.batch.svc import ProcessMultipleRecords

        pmr = ProcessMultipleRecords(is_debug=self._is_debug,
                                     d_manifest=d_manifest,
                                     xdm_schema=xdm_schema,
                                     collection_name_tag=collection_name_tag,
                                     collection_name_xdm=collection_name_xdm)

        return pmr.by_record_paging(start_record=start_record,
                                    end_record=end_record,
                                    flush_records=flush_records)

    def single_record(self,
                      d_manifest: dict,
                      key_field: str,
                      xdm_schema: str,
                      collection_name_tag: str,
                      collection_name_xdm: str,
                      mongo_host: str = None,               # deprecated/ignored
                      persist_result: bool = True):
        from cendantdim.batch.svc import ProcessSingleRecord

        client = BaseMongoClient()
        psr = ProcessSingleRecord(is_debug=self._is_debug,
                                  mongo_client=client,
                                  d_manifest=d_manifest,
                                  xdm_schema=xdm_schema,
                                  collection_name_tag=collection_name_tag,
                                  collection_name_xdm=collection_name_xdm)

        return psr.process(key_field=key_field,
                           persist_result=persist_result)
