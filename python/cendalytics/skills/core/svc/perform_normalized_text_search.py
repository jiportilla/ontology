# !/usr/bin/env python
# -*- coding: UTF-8 -*-


import pandas as pd
from pandas import DataFrame

from base import BaseObject
from datamongo import BaseMongoClient
from datamongo import PerformTextQuery


class PerformNormalizedTextSearch(BaseObject):
    """
    Purpose:
    Service that Performs a Full-Text Search on normalized text in MongoDB

    Traceability:
    https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1228

    Sample Output:
        +----+------------+---------------------------+--------------+----------------+
        |    | Division   | Result                    | SearchTerm   | SerialNumber   |
        |----+------------+-----------------------------------------------------------|
        |  0 |         | Flume, Sqoop, Hive e Spark| pyspark      | 075818631      |
        |  1 |         | Python, pySpark, Kylin,L, | pyspark      | 0497J9744      |
        |  2 |         | Business Intelligence     | pyspark      | 2J1049897      |
        |  3 |         | - 25 years of hands-on    | pyspark      | 00643N744      |
        +----+------------+-----------------------------------------------------------+

    Prereq:
    a full-text index on the MongoDB collection must exist
    """

    def __init__(self,
                 search_terms: list,
                 collection_name: str,
                 div_field: list,
                 key_field: str = None,
                 is_debug: bool = False):
        """
        Created:
            24-Apr-2019
            craig.trim@ibm.com
        Updated:
            5-Jun-2019
            craig.trim@ibm.com
            *   add division
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/324
        Updated:
            6-Aug-2019
            craig.trim@ibm.com
            *   removed unecessary 'port' parameter
        Updated:
            19-August-2019
            abhbasu3@in.ibm.com
            *   added division dict {key_field : div_field}
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/515
        Updated:
            21-August-2019
            abhbasu3@in.ibm.com
            *   added evidence `type` for each `result`
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/764
        Updated:
            28-August-2019
            iaduran@us.ibm.com
            *   wrap around error when fetching keywords that do not return results
        Updated:
            29-August-2019
            iaduran@us.ibm.com
            *   update wrap around error to allow loop to process.
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/846
        Updated:
            11-Sept-2019
            craig.trim@ibm.com
            *   pass in the collection name
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/917
        Updated:
            16-Sept-2019
            craig.trim@ibm.com
            *   pass in the key-field as a param
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/940
        Updated:
            31-Oct-2019
            craig.trim@ibm.com
            *   renamed from 'perform-skills-search'
                remove all elasticsearch references from documentation
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1224#issuecomment-15673306
        Updated:
            2-Nov-2019
            craig.trim@ibm.com
            *   update 'div-field' param to be a list instead of a str
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1201
            *   refactor 'query-by-keyfield' into another service
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
        :param collection_name:
            the target collection to query
        :param search_terms:
            a list of terms to query by
        :param div_field:
            the list of divisions (e.g., 'GTS' or 'GBS') to restrict searches for
            if None     search all divisions
            Note:
                Use the API method 'division-distribution' to find existing divisions
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

        self._div_field = div_field
        self._key_field = key_field
        self._search_terms = search_terms
        self._collection_name = collection_name
        self._mongo_client = BaseMongoClient(is_debug=is_debug)

        self.text_query = PerformTextQuery(is_debug=is_debug,
                                           div_field=div_field,
                                           key_field=key_field,
                                           mongo_client=self._mongo_client,
                                           collection_name=collection_name,
                                           use_normalized_text=True)

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
        value = a_result["type"]["name"]
        if value.lower().startswith("ingest_"):
            return value[7:].strip()
        return value

    def process(self) -> DataFrame:
        """
        :return:
            a dictionary of query results

            a single search result looks like this:
            {   'id': 'mOvBR2oBU3agQH6lI2Eg',
                'key_field': '734698897',
                'score': 1.38219,
                'source': {
                    'collection': 'supply_ingest_complete',
                    'ts': '1555961922.572545' },
                'text': 'focus is Cloud Network Architect responsible for migration of Coca '
                        'Cola Cloud services (MS Azure , Amazon AWS) from legacy data '
                        'centers to newly created IBM MWS GNPP/GNKO SD-WAN hosted in Equinex'}

            a division dictionary looks like
            {'2J2623897': 'gbs'}

        :param search_terms:
        :return:
            a DataFrame of the results
        """
        master_results = []
        for token in self._search_terms:
            try:
                # get filtered results and respective divisions
                results, division = self.text_query.process(token)

                if not results:
                    continue

                for cnum in results:
                    for result in results[cnum]:
                        master_results.append({
                            "SearchTerm": token,
                            "SerialNumber": cnum,
                            "Type": self._transform_type(result),
                            "Result": result["value"].strip(),
                            "Division": division[cnum]})
            except TypeError:
                self.logger.warning(f"No Results Found "
                                    f"(input-text={token})")

        return pd.DataFrame(master_results)
