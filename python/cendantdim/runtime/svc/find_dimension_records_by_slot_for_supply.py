#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from datamongo import BaseMongoClient
from datamongo import CendantXdm


class FindDimensionRecordsBySlotForSupply(BaseObject):

    def __init__(self,
                 collection_name: str,
                 slot_name: str,
                 region: str,
                 minimum_value_sum: float,
                 maximum_value_sum: float,
                 minimum_band_level: int,
                 maximum_band_level: int,
                 mongo_client: BaseMongoClient = None,
                 is_debug: bool = False):
        """
        Created:
            13-May-2019
            craig.trim@ibm.com
            *   refactored out of dimensions-api
        Updated:
            21-May-2019
            craig.trim@ibm.com
            *   added 'region'
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/243
        Updated:
            8-Aug-2019
            craig.trim@ibm.com
            *   removed -dimensions in favor of cendant-xdm
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/674
        Updated:
            7-Oct-2019
            craig.trim@ibm.com
            *   renamed from 'find-dimension-records-by-slot' for consistency
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1048
            *   changed source-name param to collection-name
        Updated:
            4-Nov-2019
            craig.trim@ibm.com
            *   added casing to `slot-name` parameter
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1262#issuecomment-15730493
        """
        BaseObject.__init__(self, __name__)
        if not mongo_client:
            mongo_client = BaseMongoClient()

        self._is_debug = is_debug
        self._mongo_client = mongo_client

        self._region = region
        self._slot_name = slot_name.lower().strip()
        self._minimum_value_sum = minimum_value_sum
        self._maximum_value_sum = maximum_value_sum
        self._minimum_band_level = minimum_band_level
        self._maximum_band_level = maximum_band_level

        self._cendant_xdm = CendantXdm(is_debug=self._is_debug,
                                       mongo_client=self._mongo_client,
                                       collection_name=collection_name)

    def process(self) -> list:
        records = self._cendant_xdm.by_slot_value(region=self._region,
                                                  slot_name=self._slot_name,
                                                  minimum_value_sum=self._minimum_value_sum,
                                                  maximum_value_sum=self._maximum_value_sum,
                                                  minimum_band_level=self._minimum_band_level,
                                                  maximum_band_level=self._maximum_band_level)
        if self._is_debug:
            self.logger.debug(f"Retrieved Records "
                              f"(total={len(records)})")

        return records
