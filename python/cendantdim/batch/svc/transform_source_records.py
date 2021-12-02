#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject


class TransformSourceRecords(BaseObject):
    """  """

    def __init__(self,
                 d_manifest: dict,
                 xdm_schema: str,
                 is_debug: bool = False):
        """
        Created:
            15-May-2019
            craig.trim@ibm.com
            *   this functionality was common to both
                    process-single-record
                    process-multiple-records
                reference:
                    https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/243
        Updated:
            29-Oct-2019
            craig.trim@ibm.com
            *   remove 'entity-schema-finder' in favor of new approach
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/916#issuecomment-15620801
        :param d_manifest:
        :param xdm_schema:
            the name of the schema to perform the type lookup
            Notes:
            -   typically either 'supply' or 'learning'
            -   the full list is on path 'resources/config/dimensionality'
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)

        self._is_debug = is_debug
        self._d_manifest = d_manifest
        self._xdm_schema = xdm_schema

        self.logger.debug("\n".join([
            "Instantiated TransformSourceRecords",
            "\tSchema: {}".format(self._xdm_schema)]))

    def process(self,
                source_records: list) -> dict:
        from cendantdim.batch.dmo import TagExtractor
        from cendantdim.batch.dmo import TagTransformer

        # Step: Remove Fields and Cluster Tags by Supervised, Unsupervised and System
        svcresult = TagExtractor(self._d_manifest,
                                 source_records,
                                 is_debug=self._is_debug).process()

        # Step: Associate Tags to Parents and Schema Elements
        svcresult = TagTransformer(some_results=svcresult,
                                   xdm_schema=self._xdm_schema).process()

        if self._is_debug:
            self.logger.debug("\n".join([
                "Source Record Transformation Complete",
                "\tTotal Records: {}".format(len(svcresult))]))

        return svcresult
