#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from datamongo import CendantCollection


class RetrieveSourceRecords(BaseObject):
    """  Retrieve Source Records for Feedback Sentiment Processing """

    def __init__(self,
                 collection_name: str,
                 is_debug: bool = False):
        """
        Created:
            23-Nov-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1441
        :param collection_name:
            the name of the collection to retrieve the records from
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._collection = CendantCollection(some_collection_name=collection_name)

    def process(self,
                total_records:int=None) -> list:

        def records():
            if not total_records:
                return self._collection.all()
            return self._collection.all(limit=total_records)

        records = records()
        if self._is_debug:
            self.logger.debug('\n'.join([
                f"Retrieved Records (total={len(records)})",
                f"\tCollection Name: {self._collection.collection_name}"]))

        return records
