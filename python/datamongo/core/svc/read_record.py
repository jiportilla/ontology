#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from pymongo.collection import Collection

from base import BaseObject
from base import MandatoryParamError


class ReadRecord(BaseObject):
    """ API for reading a Record (or series of Records) from MongoDB   """

    def __init__(self,
                 some_collection: Collection):
        """
        Created:
            28-Nov-2018
            craig.trim@ibm.com
        Updated:
            15-Mar-2019
            craig.trim@ibm.com
            *   add limit
        """
        BaseObject.__init__(self, __name__)
        from datamongo.core.dmo import GenericReadQueries
        from datamongo.core.dmo import BaseMongoHelper

        if not some_collection:
            raise MandatoryParamError("Collection")

        self.helper = BaseMongoHelper
        self.collection = some_collection
        self.read_queries = GenericReadQueries(some_collection)

    def all(self,
            limit: int = None) -> list:
        return self.read_queries.find(limit)

    def count(self) -> int:
        return self.collection.count_documents({})

    def skip_and_limit(self,
                       skip: int,
                       limit: int) -> list:
        return self.read_queries.skip_and_limit(skip, limit)

    def random(self,
               total_records=1) -> list:
        """ return a random record """
        cursor = self.collection.aggregate([{"$sample": {"size": total_records}}])
        return self.helper.to_result_set(cursor)

    def by_ts(self, some_ts, first_only=True):
        return self.helper.results(
            self.read_queries.find_by_ts(some_ts),
            first_only)

    def by_field(self,
                 field_name: str,
                 field_value: str):
        results = self.helper.results(
            self.read_queries.find_by_key_value(
                some_key=field_name,
                some_value=field_value))
        if results:
            return results[0]
