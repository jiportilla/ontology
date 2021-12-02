#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from pandas import DataFrame

from base import BaseObject
from datamongo import BaseMongoClient


class JrsDimensionsAPI(BaseObject):

    def __init__(self,
                 mongo_client: BaseMongoClient = None,
                 is_debug: bool = False):
        """
        Created:
            30-Sept-2019
            craig.trim@ibm.com
            *   in broad support of epic
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/983
        :param mongo_client:
            the instantiated mongoDB client instance
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        if not mongo_client:
            mongo_client = BaseMongoClient()

        self._mongo_client = mongo_client

    def lookup_job_role_id(self,
                           job_role_id: str) -> DataFrame:
        """
        Sample Input:
            042393
        Sample Output:
            +----+---------------------------------------------------------------------------------------------------------------------------------------------------------------+
            |    | Description                                     |   JobRoleId | JobRoleName         |   NumberOfSkills | PrimaryJobCategory   | SecondaryJobCategory          |
            |----+---------------------------------------------------------------------------------------------------------------------------------------------------------------|
            |  0 | This role supervises, coordinates and maintains |      042393 | Service Coordinator |                9 | Technical Specialist | Technical Services Specialist |
            +----+---------------------------------------------------------------------------------------------------------------------------------------------------------------+
        :param job_role_id:
            a Job Role ID to search on
        :return:
            a DataFrame of results
        """
        from cendantdim.jrs.svc import LookupJobRoleId

        return LookupJobRoleId(job_role_id=job_role_id,
                               mongo_client=self._mongo_client,
                               is_debug=self._is_debug).process()

    def analyze(self,
                df_means: DataFrame,
                df_cnums: DataFrame,
                mongo_client: BaseMongoClient) -> DataFrame:
        """
        :param df_means:
            +----+---------------------+---------+----------+
            |    | Slot                |   Stdev |   Weight |
            |----+---------------------+---------+----------|
            |  0 | Blockchain          |   0.977 |    0.226 |
            |  1 | Cloud               |   4.46  |    2.683 |
            |  2 | DataScience         |  10.156 |    5.492 |
            |  3 | Database            |   6.858 |    3.404 |
            |  4 | HardSkill           |  16.744 |   19.523 |
            |  5 | Other               |  10.075 |    8.86  |
            |  6 | ProjectManagement   |  12.84  |   11.175 |
            |  7 | Quantum             |   0.107 |    0.032 |
            |  8 | ServiceManagement   |  13.014 |   11.658 |
            |  9 | SoftSkill           |  16.234 |   18.07  |
            | 10 | SystemAdministrator |  12.323 |    8.431 |
            +----+---------------------+---------+----------+
        :param df_cnums:
            +------+-------------+----------------+---------------------+----------+
            |      |   JobRoleId | SerialNumber   | Slot                |   Mean   |
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
        :param mongo_client:
            an instantiated mongo client
        :return:
        """
        from cendantdim.jrs.svc import GenerateZcores

        return GenerateZcores(df_means=df_means,
                              df_cnums=df_cnums,
                              mongo_client=mongo_client,
                              is_debug=self._is_debug).process()

    def mean(self,
             records: list or DataFrame):
        """
        Purpose:
            Generate a Mean of Dimensionality Records
        Reference:
            https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1010
        :param records:
            as list
                any list of dimensionality records
                (e.g., records from an XDM collection)
            as DataFrame
                the output from 'mapping' method of this API
                +------+-------------+----------------+---------------------+----------+
                |      |   JobRoleId | SerialNumber   | Slot                |   Mean   |
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

        :return:
            a single record that contains a mean of the slot values across all incoming records
        """
        from cendantdim.jrs.svc import GenerateMeansRecord
        return GenerateMeansRecord(records=records,
                                   is_debug=self._is_debug).process()

    def mapping(self,
                collection_name: str,
                job_role_id: str = None,
                division: str = None,
                skip: int = 0,
                limit: int = 0) -> DataFrame:
        """
        Purpose:
            return a result that maps existing XDM records to JRS IDs
        Sample Output (abbreviated):
            +------+-------------+----------------+---------------------+----------+
            |      |   JobRoleId | SerialNumber   | Slot                |   Mean   |
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
        :return:
            a dataframe of mappings
        """
        from cendantdim.jrs.svc import GenerateJrsMapping

        return GenerateJrsMapping(collection_name=collection_name,
                                  job_role_id=job_role_id,
                                  division=division,
                                  skip=skip,
                                  limit=limit,
                                  mongo_client=self._mongo_client,
                                  is_debug=self._is_debug).process()


if __name__ == "__main__":
    df2 = (
        JrsDimensionsAPI(mongo_client=BaseMongoClient(), is_debug=True).lookup_job_role_id(job_role_id='042393'))
    from tabulate import tabulate

    print(tabulate(df2,
                   headers='keys',
                   tablefmt='psql'))
