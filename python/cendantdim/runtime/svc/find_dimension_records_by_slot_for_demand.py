#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pprint

from base import BaseObject
from datamongo import BaseMongoClient
from datamongo import CendantXdm
from datamongo import CollectionFinder
from datamongo import GenerateSlotQuery


class FindDimensionRecordsBySlotForDemand(BaseObject):

    def __init__(self,
                 collection_name: str,
                 slot_name: str,
                 region: str,
                 minimum_value_sum: float,
                 maximum_value_sum: float,
                 minimum_band_level: int,
                 maximum_band_level: int,
                 status: str = None,
                 start_date: str = None,
                 end_date: str = None,
                 mongo_client: BaseMongoClient = None,
                 is_debug: bool = False):
        """
        Created:
            21-May-2019
            craig.trim@ibm.com
            *   added 'region'
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/243
        Updated:
            8-Aug-2019
            craig.trim@ibm.com
            *   removed -dimensions and added cendant-xdm
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/674
        Updated:
            27-Sept-2019
            *   added mongo collection name as input parameter
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1004
        """
        BaseObject.__init__(self, __name__)
        self.is_debug = is_debug
        if not mongo_client:
            mongo_client = BaseMongoClient()

        self.mongo_client = mongo_client

        self.region = region
        self.status = status
        self.slot_name = slot_name
        self.start_date = start_date
        self.end_date = end_date
        self.minimum_value_sum = minimum_value_sum
        self.maximum_value_sum = maximum_value_sum
        self.minimum_band_level = minimum_band_level
        self.maximum_band_level = maximum_band_level

        self._cendant_xdm = CendantXdm(is_debug=self.is_debug,
                                       mongo_client=mongo_client,
                                       collection_name=collection_name)

    def _slot_query(self) -> dict:
        generator = GenerateSlotQuery(is_debug=self.is_debug)
        return generator.process(region=self.region,
                                 slot_name=self.slot_name,
                                 minimum_value_sum=self.minimum_value_sum,
                                 maximum_value_sum=self.maximum_value_sum,
                                 minimum_band_level=self.minimum_band_level,
                                 maximum_band_level=self.maximum_band_level)

    def _date_range_query(self) -> dict:

        if self.start_date and self.end_date:
            start_date = '{} 00:00:00'.format(self.start_date)
            end_date = '{} 00:00:00'.format(self.end_date)

            return {'$and': [
                {'fields.date.start_date': {'$gte': start_date}},
                {'fields.date.end_date': {'$lte': end_date}}]}

        elif self.start_date:
            start_date = '{} 00:00:00'.format(self.start_date)
            return {'$and': [
                {'fields.date.start_date': {'$gte': start_date}}]}

        elif self.end_date:
            end_date = '{} 00:00:00'.format(self.end_date)
            return {'$and': [
                {'fields.date.end_date': {'$lt': end_date}}]}

        raise NotImplementedError

    def process(self):
        d_query = self._slot_query()

        if self.start_date or self.end_date:
            d_date_query = self._date_range_query()
            for query_component in d_date_query['$and']:
                d_query['$and'].append(query_component)

        if self.status:
            d_query['$and'].append({'fields.status': self.status})

        results = self._cendant_xdm.collection.find_by_query(d_query)
        if not results:
            results = []

        self.logger.debug("\n".join([
            "Slot Value Query (results={})".format(len(results)),
            pprint.pformat(d_query, indent=4)]))

        return results
