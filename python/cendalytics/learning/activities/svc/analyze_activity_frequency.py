# !/usr/bin/env python
# -*- coding: UTF-8 -*-


import pandas as pd
from pandas import DataFrame

from base import BaseObject
from datamongo import BaseMongoClient
from datamongo import CendantTag
from datamongo import PerformTextQuery


class AnalyzeActivityFrequency(BaseObject):
    """
    Purpose:
    Service that Performs a Skills-based Search against ElasticSearch
    using Graph-based inference

    Traceability:
    https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/141

    Prereq:
    an ElasticSearch index of the text must exist
    """

    def __init__(self,
                 collection_name: str,
                 host: str = None,  # deprecated/ignored
                 div_field: str = None,
                 key_field: str = None,
                 use_normalized_text: bool = True,
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
        :param collection_name:
            the target collection to query
        :param use_normalized_text:
            True        query on normalized text
            False       query on original (raw) text
        :param div_field:
            the division to filter against (GTS or GBS)
        :param key_field:
            the key field to restrict the search on
            if None     search all key fields
            Note:       the meaning of the key-field varies by collection
                        supply_tag_*    the "key field" is the Serial Number
                        demand_tag_*    the "key field" is the Open Seat ID
                        learning_tag_*  the "key field" is the Learning Activity ID
        :param host:
            deprecated/ignored
        :param is_debug:
            if True     increase log output at DEBUG level
        """
        BaseObject.__init__(self, __name__)

        self._div_field = div_field
        self._key_field = key_field
        self._collection_name = collection_name
        self._mongo_client = BaseMongoClient(is_debug=is_debug)

        if type(div_field) == str:
            div_field = [div_field]

        self.text_query = PerformTextQuery(is_debug=is_debug,
                                           div_field=div_field,
                                           key_field=key_field,
                                           mongo_client=self._mongo_client,
                                           collection_name=collection_name,
                                           use_normalized_text=use_normalized_text)

    def query_by_keyfield(self) -> DataFrame:
        """
        Purpose:
            Find all the Skills (tags) by a Key Field (e.g., Serial Number)
        :return:
            a DataFrame of the results
        """
        master_results = []
        tag_collection = CendantTag(mongo_client=self._mongo_client,
                                    collection_name=self._collection_name)
        svcresult = tag_collection.collection.by_key_field(self._key_field)  # GIT-1415-16099357

        fields = [field for field in svcresult["fields"] if field["type"] == "long-text"]
        fields = [field for field in fields if "tags" in field and len(field["tags"]["supervised"])]

        for field in fields:
            for tag_tuple in field["tags"]["supervised"]:
                master_results.append({
                    "SearchTerm": None,
                    "SerialNumber": svcresult["key_field"],
                    "Type": field["type"],
                    "Result": tag_tuple[0],
                    "Division": svcresult["div_field"]})

        return pd.DataFrame(master_results)

    def query_by_search_terms(self,
                              search_terms: list) -> DataFrame:
        """
        :param search_terms:
            a list of terms to query by
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
        for token in search_terms:
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
                            "Type": result["type"],
                            "Result": result["value"].strip(),
                            "Division": division[cnum]})
            except TypeError:
                self.logger.warning(f"No Results Found "
                                    f"(input-text={token})")

        return pd.DataFrame(master_results)
