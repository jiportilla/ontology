#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from pandas import DataFrame

from base import BaseObject
from datamongo import BaseMongoClient
from datamongo import IngestJrs


class LookupJobRoleId(BaseObject):
    """
    Purpose:
        Perform a Lookup into the JRS collection
    Sample Input:
        042393
    Sample Output:
        +----+---------------------------------------------------------------------------------------------------------------------------------------------------------------+
        |    | Description                                     |   JobRoleId | JobRoleName         |   NumberOfSkills | PrimaryJobCategory   | SecondaryJobCategory          |
        |----+---------------------------------------------------------------------------------------------------------------------------------------------------------------|
        |  0 | This role supervises, coordinates and maintains |      042393 | Service Coordinator |                9 | Technical Specialist | Technical Services Specialist |
        +----+---------------------------------------------------------------------------------------------------------------------------------------------------------------+
    """

    def __init__(self,
                 job_role_id: str,
                 mongo_client: BaseMongoClient,
                 is_debug: bool = False):
        """
        Created:
            1-Oct-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1033
        Updated:
            3-Oct-2019
            craig.trim@ibm.com
            *   add description abbreviation
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1039
        :param job_role_id:
            a Job Role ID to search on
        :return:
            a DataFrame of results
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._job_role_id = job_role_id
        self._ingest_jrs_col = IngestJrs(mongo_client=mongo_client,
                                         is_debug=is_debug)

    # @staticmethod
    # def _description(description: str):
    #     """
    #     Purpose:
    #         Abbreviate lengthy descriptions which can often be >5000 chars in length
    #     :param description:
    #         a JRS description of any length
    #     :return:
    #         an abbreviated description
    #     """
    #
    #     if '.' in description:
    #         description = description[:description.index('.')]
    #
    #         if len(description) > 250 and '.' in description:
    #             description = description[:description.index('.')]
    #
    #     # split nicely on a space
    #     if len(description) > 250:
    #
    #         result = []
    #         ctr = 0
    #         tokens = description.split(' ')
    #         for token in tokens:
    #             result.append(token)
    #             ctr += len(token)
    #             if ctr > 250:
    #                 return ' '.join(result)
    #
    #     return description

    def process(self) -> DataFrame:
        return self._ingest_jrs_col.find_by_job_role_id(self._job_role_id)
        # results = []
        # for record in self._ingest_jrs_col.find_by_job_role_id(self._job_role_id):
        #     fields = record["fields"]
        #
        #     def _by_field_name(a_field_name: str) -> str:
        #         return [field for field in fields if field["name"] == a_field_name][0]["value"]
        #
        #     try:
        #         description = self._description(_by_field_name('description'))
        #     except IndexError:
        #         print(fields)
        #
        #     results.append({
        #         "JobRoleId": self._job_role_id,
        #         "JobRoleName": _by_field_name("name"),
        #         "Description": description,
        #         "PrimaryJobCategory": _by_field_name("primary_job_category"),
        #         "SecondaryJobCategory": _by_field_name("secondary_job_category"),
        #         "NumberOfSkills": _by_field_name("number_of_skills")})
        #
        # return pd.DataFrame(results)
