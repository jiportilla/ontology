#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject

from datamongo import CendantCollection


class BadgeAnalyticsGenerator(BaseObject):
    """ Generate the badge analytics collection into a dictionary
        this is an association of known badges to Cendant tags
    """

    def __init__(self,
                 collection_name: str = 'analytics_badges'):
        """
        Created:
            13-Oct-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1098#issuecomment-15265293
        """
        BaseObject.__init__(self, __name__)
        self._collection_name = collection_name

    def process(self) -> dict:
        records = CendantCollection(some_collection_name=self._collection_name).all()
        if not records or not len(records):
            raise NotImplementedError(f"Analytics Badges Collection is Empty "
                                      f"(name={self._collection_name})")

        d_results = {}
        for record in records:
            if len(record["tags"]):
                d_results[record["badge"]] = record["tags"]

        return d_results
