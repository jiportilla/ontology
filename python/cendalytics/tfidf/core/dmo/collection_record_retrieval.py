#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import time

from base import BaseObject
from datamongo import BaseMongoClient
from datamongo import CendantCollection


class CollectionRecordRetrieval(BaseObject):
    """ Retrieve records from source collection """

    def __init__(self,
                 collection_name: str,
                 division: str or None,
                 mongo_client: BaseMongoClient,
                 limit: int = 0,
                 is_debug: bool = False):
        """
        Created:
            6-Nov-2019
            craig.trim@ibm.com
            *   refactored out of 'create-collection-vectorspace'
        """
        BaseObject.__init__(self, __name__)
        self._limit = limit
        self._is_debug = is_debug
        self._division = division
        self._collection = CendantCollection(is_debug=self._is_debug,
                                             some_base_client=mongo_client,
                                             some_collection_name=collection_name)

    def process(self) -> list:
        start = time.time()

        def inner_process() -> list:
            if self._division:
                q = {"div_field": self._division.lower()}
                return self._collection.find_by_query(q, limit=self._limit)
            return self._collection.all(limit=self._limit)

        records = inner_process()

        if self._is_debug:
            end = round(time.time() - start, 2)
            if not records or not len(records):
                self.logger.debug('\n'.join([
                    "No Records Found",
                    f"\tTotal Time: {end}s",
                    f"\tDivision: {self._division}",
                    f"\tCollection: {self._collection.collection_name}"]))
            else:
                self.logger.debug('\n'.join([
                    "Retrieved Records",
                    f"\tTotal Time: {end}s",
                    f"\tLimit: {self._limit}s",
                    f"\tDivision: {self._division}",
                    f"\tTotal Records: {len(records)}",
                    f"\tCollection: {self._collection.collection_name}"]))

        return records
