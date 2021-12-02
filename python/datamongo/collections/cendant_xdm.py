#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pprint

import pandas as pd
from pandas import DataFrame

from base import BaseObject
from base import MandatoryParamError
from datamongo.core.bp import CendantCollection
from datamongo.core.dmo import BaseMongoClient


class CendantXdm(BaseObject):
    """ Collection Wrapper over MongoDB XDM (Dimemsionality) Collections
        *   supply-xdm
        *   demand-xdm
        *   learning-xdm    """

    _records = None

    def __init__(self,
                 collection_name: str,
                 mongo_client: BaseMongoClient = None,
                 database_name: str = "cendant",
                 is_debug: bool = True):
        """
        Created:
            7-Aug-2019
            craig.trim@ibm.com
            *   based on 'cendant-tag' and 'cendant-src'
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/649
        Updated:
            30-Sept-2019
            craig.trim@ibm.com
            *   filter out records without a JRS ID
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1028
        Updated:
            31-Oct-2019
            craig.trim@ibm.com
            *   add 'dataframe' method
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1227#issuecomment-15665110
        """
        BaseObject.__init__(self, __name__)
        if not collection_name:
            raise MandatoryParamError("Collection Name")
        if not mongo_client:
            mongo_client = BaseMongoClient()

        self.is_debug = is_debug
        self.mongo_client = mongo_client
        self.collection = CendantCollection(some_base_client=mongo_client,
                                            some_db_name=database_name,
                                            some_collection_name=collection_name,
                                            is_debug=self.is_debug)

    def random(self,
               total_records: int = 1) -> list:
        """
        Purpose:
            Return Random Record(s)
        :param total_records:
            the number of random records to return
        :return:
            a list of random records with a cardinality of 0..*
        """
        return self.collection.random(total_records)

    @staticmethod
    def dataframe(record: dict) -> DataFrame:
        results = []
        key_field = record["key_field"]
        for slot_name in record["slots"]:
            results.append({
                "Id": key_field,
                "Slot": slot_name,
                "Weight": record["slots"][slot_name]["weight"],
                "zScore": record["slots"][slot_name]["z_score"],
                "zScoreNorm": record["slots"][slot_name]["z_score_norm"]})

        return pd.DataFrame(results)

    def by_value_sum(self,
                     minimum_value_sum: int = None,
                     maximum_value_sum: int = None,
                     key_fields_only: bool = False) -> list:
        from datamongo.slots.dmo import SlotValueFilter

        slot_value_filter = SlotValueFilter(some_records=self.collection.all())
        return slot_value_filter.process(minimum_value_sum=minimum_value_sum,
                                         maximum_value_sum=maximum_value_sum,
                                         key_fields_only=key_fields_only)

    def by_slot_value(self,
                      region: str,
                      slot_name: str,
                      minimum_value_sum: float = None,
                      maximum_value_sum: float = None,
                      minimum_band_level: int = None,
                      maximum_band_level: int = None) -> list:
        from datamongo.slots.svc import GenerateSlotQuery

        slot_query = GenerateSlotQuery(is_debug=self.is_debug)
        d_query = slot_query.process(region=region,
                                     slot_name=slot_name,
                                     minimum_value_sum=minimum_value_sum,
                                     maximum_value_sum=maximum_value_sum,
                                     minimum_band_level=minimum_band_level,
                                     maximum_band_level=maximum_band_level)

        results = self.collection.find_by_query(d_query)
        if not results:
            results = []

        self.logger.debug("\n".join([
            f"Slot Value Query (total={len(results)}): ",
            pprint.pformat(d_query, indent=4)]))

        return results
