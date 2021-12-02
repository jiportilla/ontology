#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from typing import Optional

from pymongo.collection import Collection
from pymongo.cursor import Cursor

from base import BaseObject
from base import MandatoryParamError


class BaseMongoHelper(BaseObject):
    """ Helper methods for MongoDB """

    def __init__(self,
                 some_collection: Collection):
        """
        Created:
            28-Nov-2018
            craig.trim@ibm.com
        Updated:
            26-Jun-2019
            craig.trim@ibm.com
            *   removed the default 'sort' options
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/379
        Updated:
            10-Oct-2019
            craig.trim@ibm.com
            *   try adding 'cursor.close()' to 'to-result-set'
        Updated:
            24-Nov-2019
            xavier.verges@es.ibm.com
            *   prevent cursors from timing out, and close them
            *   delete/mark unused functions that are not used
        """
        BaseObject.__init__(self, __name__)
        if not some_collection:
            raise MandatoryParamError("Collection")

        self.collection = some_collection

    def query(self,
              query,
              first_only=False,
              is_debug=False,
              limit=None):

        if limit and not isinstance(limit, int):
            raise ValueError("\n".join([
                "Unexpected Datatype for 'limit'",
                "\tactual: {0}".format(type(limit)),
                "\texpected: int"
            ]))

        if not limit:
            limit = 0

        def _results():
            if first_only:
                return self.collection.find_one(query)
            else:
                no_autoclose_cursor = self.collection.find(filter=query,
                                                           no_cursor_timeout=True,
                                                           limit=limit)
                with no_autoclose_cursor:
                    results = self.to_result_set(no_autoclose_cursor)
                    return self.results(results)

        results = _results()

        if is_debug:
            self.logger.debug(self.get_log_stmt(
                self.collection, query, results))

        return results

    def distinct(self,
                 field,
                 force_lower=True,
                 is_debug=False):
        """ return distinct results for a field """

        results = self.collection.distinct(field)
        results = [str(x) for x in results if x]
        results = [x.strip() for x in results if len(x) > 0]
        if force_lower:
            results = [x.lower() for x in results]

        if is_debug:
            self.logger.debug(
                self.get_log_stmt(self.collection,
                                  field,
                                  results))

        return results

    @staticmethod
    def index_find(collection: Collection,
                   query: dict,
                   limit: int = None) -> Cursor:
        "Unused"
        if limit:
            return collection.find(query).limit(limit)
        return collection.find(query)

    @staticmethod
    def skip_and_limit(collection: Collection,
                       skip: int,
                       limit: int,
                       find_options: dict = None) -> Cursor:
        if not find_options:
            find_options = {}
        return collection.find({}, **find_options).skip(skip).limit(limit)

    @staticmethod
    def generate_text_query(add_query, search_text):
        "Unused"
        search_dict = {'$text': {'$search': search_text}}
        if add_query:
            search_dict.update(add_query)

        return search_dict

    @staticmethod
    def get_log_stmt(collection: Collection,
                     query,
                     results):
        if results is None:
            return "No Results Found '{0}':\n\ttotal = 0\n\tquery = {1}".format(
                collection.name,
                query)

        def get_total():
            if isinstance(results, dict):
                return 1
            return len(results)

        return "Located Results in '{0}':\n\ttotal = {1}\n\tquery = {2}".format(
            collection.name,
            get_total(),
            query)

    @staticmethod
    def to_result_set(cursor: Cursor) -> list:

        def replace_str_none_by_obj_none(some_dict: dict) -> None:
            for key in some_dict:
                v = some_dict[key]
                if isinstance(v, dict):
                    replace_str_none_by_obj_none(v)
                elif v == "None":
                    some_dict[key] = None

        results = []
        for value in cursor:
            replace_str_none_by_obj_none(value)
            results.append(value)

        # cursor.close()
        return results

    @staticmethod
    def has(some_results: list) -> bool:
        return some_results is not None and len(some_results) != 0

    @staticmethod
    def results(some_results: list,
                required_cardinality=None) -> Optional[list]:
        """
        Purpose:
            throw an exception if the results do not match the required cardinality
            Cardinality is either
                None or Integer
            Cardinality is None by default
        :return:
            the results
        """

        if some_results is None or not len(some_results):
            return None

        if required_cardinality is None:
            return some_results

        if not isinstance(required_cardinality, int):
            raise ValueError

        if len(some_results) != required_cardinality:
            raise ValueError

        return some_results
