# !/usr/bin/env python
# -*- coding: UTF-8 -*-


import pandas as pd
from pandas import DataFrame

from base import BaseObject
from datamongo import BaseMongoClient
from datamongo import CendantTag


class PerformKeyFieldSearch(BaseObject):
    """ Facade: Service that Performs a KeyField search in MongoDB """

    def __init__(self,
                 collection_name: str,
                 key_field: str = None,
                 is_debug: bool = False):
        """
        Created:
            2-Nov-2019
            craig.trim@ibm.com
            *   refactored out of 'perform-normalized-text-search'
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1228#issuecomment-15702196
        Updated:
            3-Nov-2019
            craig.trim@ibm.com
            *   add the 'transform-type' method
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1234#issuecomment-15707274
        Updated:
            8-Nov-2019
            craig.trim@ibm.com
            *   align with facade pattern
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1293
        Updated:
            11-Nov-2019
            craig.trim@ibm.com
            *   add additional logging if key-field not found in pursuit of
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1316
        :param collection_name:
            the target collection to query
        :param key_field:
            the key field to restrict the search on
            if None     search all key fields
            Note:       the meaning of the key-field varies by collection
                        supply_tag_*    the "key field" is the Serial Number
                        demand_tag_*    the "key field" is the Open Seat ID
                        learning_tag_*  the "key field" is the Learning Activity ID
        :param is_debug:
            if True     increase log output at DEBUG level
        """
        BaseObject.__init__(self, __name__)

        self._is_debug = is_debug
        self._key_field = key_field
        self._collection_name = collection_name
        self._mongo_client = BaseMongoClient(is_debug=is_debug)

    @staticmethod
    def _transform_type(a_result: dict) -> str:
        """
        Purpose:
            Remove 'ingest_' prefix
        Reference:
            https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1234#issuecomment-15707274
        Sample Input:
            ingest_career_history_
        Sample Output:
            career_history_
        :return:
            a transformed string
        """
        value = a_result["collection"]["name"]
        if value.lower().startswith("ingest_"):
            return value[7:].strip()
        return value

    def process(self) -> DataFrame or None:
        """
        Purpose:
            Find all the Skills (tags) by a Key Field (e.g., Serial Number)
        :return:
            DataFrame   if a result is returned
            None        if no result found
        """
        master_results = []
        tag_collection = CendantTag(is_debug=self._is_debug,
                                    mongo_client=self._mongo_client,
                                    collection_name=self._collection_name)

        svcresult = tag_collection.collection.by_key_field(self._key_field)  # GIT-1415-16099357
        if not svcresult:
            self.logger.warning(f"Key Field Not Found ("
                                f"collection={self._collection_name}, "
                                f"key-field={self._key_field})")
            return None

        fields = [field for field in svcresult["fields"] if field["type"] == "long-text"]
        fields = [field for field in fields if "tags" in field and len(field["tags"]["supervised"])]

        if not len(fields) and self._is_debug:
            self.logger.debug(f"No Annotation Fields Located "
                              f"(key-field={self._key_field})")

        for field in fields:
            for tag_tuple in field["tags"]["supervised"]:
                master_results.append({
                    "SearchTerm": None,
                    "Result": tag_tuple[0],
                    "Division": svcresult["div_field"],
                    "Type": self._transform_type(field),
                    "SerialNumber": svcresult["key_field"]})

        return pd.DataFrame(master_results)
