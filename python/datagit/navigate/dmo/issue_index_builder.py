# !/usr/bin/env python
# -*- coding: UTF-8 -*-


import pprint
from typing import Optional

from base import BaseObject
from datamongo import CendantCollection


class IssueIndexBuilder(BaseObject):
    """ Build an Index of records for a given GitHub Issue

    Sample Output:
        {   '1111': { ... },
            '1111-15347705': { ... },
            '1111-15347724': { ... },
            ...
            '1111-66735318-MDY6Q29tbWl0NTA4MzYzOjAxODMyZmY3MTYzNDY3NDUyY2JlYWQzNTAyNTM4NDhhYTAwMWU4ZGM=-9': { ... } }

    Prereq:
        The source GitHub source collection must exist
    """

    _graphviz = []

    __collection = None

    def __init__(self,
                 collection_name: str,
                 is_debug: bool = True):
        """
        Created:
            6-Dec-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1542
        Updated:
            17-Dec-2019
            craig.trim@ibm.com
            *   refactored out of 'navigate-github-records'
                https://github.ibm.com/-cdo/unstructured-analytics/issues/1543#issuecomment-16682057
        Updated:
            25-Dec-2019
            craig.trim@ibm.com
            *   process flow optimizations in pursuit of
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1651
        :param is_debug:
            if True     increase log output at DEBUG level
        """
        BaseObject.__init__(self, __name__)

        self._is_debug = is_debug

        if self.__collection is None:
            self.__collection = CendantCollection(is_debug=False,
                                                  some_collection_name=collection_name)

    def _child_records(self,
                       a_record: dict) -> list:

        children = self.__collection.by_field(field_name="key_field_parent",
                                             field_value=a_record['key_field'])
        if not children:
            return []
        return children

    def process(self,
                key_field: str) -> Optional[dict]:

        d_index = {}
        s_unique = set()

        issue = self.__collection.by_key_field(key_field)
        if not issue:
            self.logger.warning('\n'.join([
                "No Issue Found",
                f"\tKey Field: {key_field}",
                f"\tCollection: {self.__collection.collection_name}"]))
            return None

        def _iter_children(a_record: dict) -> None:

            d_index[a_record['key_field']] = a_record
            s_unique.add(a_record['key_field'])

            children = self._child_records(a_record)
            children = [child for child in children
                        if child['key_field'] not in s_unique]

            for child in children:
                s_unique.add(child['key_field'])
                _iter_children(child)

        _iter_children(issue)

        if self._is_debug:
            self.logger.debug('\n'.join([
                f"Index Completed (issue={key_field})",
                pprint.pformat(d_index, indent=4)]))

        return d_index
