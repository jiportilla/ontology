#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from pymongo.collection import Collection

from base import BaseObject
from base import MandatoryParamError
from datamongo.core.dmo import BaseMongoHelper


class GenericReadQueries(BaseObject):
    """ Generic Read queries for MongoDB """

    def __init__(self,
                 some_collection: Collection):
        """
        Created:
            28-Nov-2018
            craig.trim@ibm.com
        Updated:
            15-Mar-2019
            craig.trim@ibm.com
            *   add limit and strict typing
        Updated:
            24-Nov-2019
            xavier.verges@es.ibm.com
            *   prevent cursors from timing out, and close them
        :param some_collection:
        """
        BaseObject.__init__(self, __name__)

        if not some_collection:
            raise MandatoryParamError("Collection")

        self.collection = some_collection
        self.helper = BaseMongoHelper(some_collection)

    def find_by_query(self,
                      some_query: dict,
                      limit: int = None) -> list:
        """
        Purpose:
            Generic Find Method
        :param some_query:
            a valid and well-formed mongoDB JSON query
        :param limit:
            (optional) the limit of records to return
        :return:
            slack event result set
        """
        if not limit:
            limit = 0
        no_autoclose_cursor =  self.collection.find(filter=some_query,
                                                     no_cursor_timeout=True,
                                                     limit=limit)
        with no_autoclose_cursor:
            return self.helper.to_result_set(no_autoclose_cursor)

    def find(self,
             limit: int = None) -> list:
        return self.find_by_query(some_query={},
                                  limit=limit)

    def skip_and_limit(self,
                       skip: int,
                       limit: int) -> list:
        def _cursor():
            return BaseMongoHelper.skip_and_limit(self.collection,
                                                  skip,
                                                  limit)

        no_autoclose_cursor =  BaseMongoHelper.skip_and_limit(self.collection,
                                                              skip,
                                                              limit,
                                                              find_options={'no_cursor_timeout': True})
        with no_autoclose_cursor:
            return self.helper.to_result_set(no_autoclose_cursor)

    def find_by_id(self,
                   some_id: str) -> list:
        return self.find_by_key_value("_id", some_id)

    def find_by_ts(self,
                   some_ts: str) -> list:
        return self.find_by_key_value("ts", some_ts)

    def find_by_key_value(self,
                          some_key: str,
                          some_value: str) -> list:
        return self.find_by_query({some_key: some_value})
