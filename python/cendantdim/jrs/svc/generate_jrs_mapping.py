#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pandas as pd
from pandas import DataFrame

from base import BaseObject
from datamongo import BaseMongoClient
from datamongo import CendantXdm


class GenerateJrsMapping(BaseObject):
    """
    Purpose:
        Generate a DataFrame where each row contains an XDM

    Sample Output:
        +------+-------------+----------------+---------------------+----------+
        |      |   JobRoleId | SerialNumber   | Slot                |   Weight |
        |------+-------------+----------------+---------------------+----------|
        |    0 |      042393 | 050484766      | Blockchain          |   0      |
        |    1 |      042393 | 050484766      | Quantum             |   0      |
        |    2 |      042393 | 050484766      | Cloud               |   0      |
        |    3 |      042393 | 050484766      | SystemAdministrator |   4.022  |
        |    4 |      042393 | 050484766      | Database            |   0      |
        ...
        | 1098 |      042393 | 9D5863897      | ProjectManagement   |   0      |
        | 1099 |      042393 | 9D5863897      | ServiceManagement   |   4.411  |
        +------+-------------+----------------+---------------------+----------+
    """

    def __init__(self,
                 collection_name: str,
                 job_role_id: str = None,
                 division: str = None,
                 skip: int = 0,
                 limit: int = 0,
                 mongo_client: BaseMongoClient = None,
                 is_debug: bool = False):
        """
        Created:
            30-Sept-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1030
        Updated:
            1-Oct-2019
            craig.trim@ibm.com
            *   transposed DataFrame
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1029
        Updated:
            3-Oct-2019
            craig.trim@ibmn.com
            *   add division as an optional query parameter
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1044
        :param collection_name:
            the source collection to search in
                e.g.,  Supply, Demand, Learning (case insensitive)
        :param job_role_id:
            the JRS ID to query on
            Optional    if None, return all records regardless of JRS ID
        :param division:
            the division to restrict the query by (e.g., GTS, GBS, System, Cloud)
            Optional    if None, return all records regarless of division
        :param skip:
            the number of records to skip
            Optional    if None, use 0
        :param limit:
            the total number of records to return
            Optional    if None, return all
        :param mongo_client:
            the instantiated mongoDB client instance
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)
        self.is_debug = is_debug
        if not mongo_client:
            mongo_client = BaseMongoClient()

        self._skip = skip
        self._limit = limit
        self._division = division
        self._job_role_id = job_role_id
        self.mongo_client = mongo_client
        self._collection_name = collection_name

    def _query(self):
        """
        :return:
            the XDM records
        """
        cendant_xdm = CendantXdm(collection_name=self._collection_name,
                                 mongo_client=self.mongo_client,
                                 database_name='cendant',
                                 is_debug=self.is_debug)

        query = {}
        if self._job_role_id:
            query["fields.job_role_id"] = self._job_role_id.upper()
        if self._division:
            query["fields.division"] = self._division.lower()

        return cendant_xdm.collection.find_by_query(some_query=query,
                                                    limit=self._limit)

        # return cendant_xdm.all(skip=self._skip,
        #                        limit=self._limit,
        #                        jrs_id_required=True)

    @staticmethod
    def _to_dataframe(records: list) -> DataFrame:
        """
        :param records:
            the queried records
        :return:
            a DataFrame of results
        """
        results = []
        for record in records:
            for slot in record["slots"]:
                _title = str.title(slot).replace(' ', '')
                results.append({
                    "Slot": _title,
                    "Weight": record["slots"][slot]["weight"],
                    "SerialNumber": record["key_field"],
                    "JobRoleId": record["fields"]["job_role_id"]})
        return pd.DataFrame(results)

    def process(self) -> DataFrame:
        """
        Purpose:
            Return dimension records by key-field
        :return:
            a DataFrame with the records
            Sample Output (abbreviated):
                +------+-------------+----------------+---------------------+----------+
                |      |   JobRoleId | SerialNumber   | Slot                |   Weight |
                |------+-------------+----------------+---------------------+----------|
                |    0 |      042393 | 050484766      | Blockchain          |   0      |
                |    1 |      042393 | 050484766      | Quantum             |   0      |
                |    2 |      042393 | 050484766      | Cloud               |   0      |
                |    3 |      042393 | 050484766      | SystemAdministrator |   4.022  |
                |    4 |      042393 | 050484766      | Database            |   0      |
                ...
                | 1098 |      042393 | 9D5863897      | ProjectManagement   |   0      |
                | 1099 |      042393 | 9D5863897      | ServiceManagement   |   4.411  |
                +------+-------------+----------------+---------------------+----------+
        """
        return self._to_dataframe(self._query())
