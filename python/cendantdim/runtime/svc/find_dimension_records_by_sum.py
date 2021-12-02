#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from datamongo import BaseMongoClient
from datamongo import CendantXdm
from datamongo import CollectionFinder


class FindDimensionRecordsBySum(BaseObject):

    def __init__(self,
                 mongo_client: BaseMongoClient = None,
                 is_debug: bool = False):
        """
        Created:
            13-May-2019
            craig.trim@ibm.com
            *   refactored out of dimensions-api
        Updated:
            8-Aug-2019
            craig.trim@ibm.com
            *   removed -dimensions in favor of cendant-xdm
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/674
        """
        BaseObject.__init__(self, __name__)
        if not mongo_client:
            mongo_client = BaseMongoClient()

        self._is_debug = is_debug
        self.mongo_client = mongo_client

    def process(self,
                source_name: str,
                minimum_value_sum: int,
                maximum_value_sum: int,
                key_fields_only: bool) -> list:
        cendant_xdm = CendantXdm(collection_name=CollectionFinder.find_xdm(source_name),
                                 mongo_client=self.mongo_client,
                                 is_debug=self._is_debug)

        return cendant_xdm.by_value_sum(minimum_value_sum=minimum_value_sum,
                                        maximum_value_sum=maximum_value_sum,
                                        key_fields_only=key_fields_only)
