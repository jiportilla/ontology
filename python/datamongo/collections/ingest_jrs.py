#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pandas as pd
from pandas import DataFrame

from base import BaseObject
from datamongo.core.bp import CendantCollection
from datamongo.core.dmo import BaseMongoClient


class IngestJrs(BaseObject):
    """
    Purpose:
    """

    def __init__(self,
                 mongo_client: BaseMongoClient,
                 collection_name: str = 'ingest_jrs',
                 is_debug: bool = False):
        """
        Created:
            1-Oct-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1033
        :param mongo_client:
            an instantiated mongoDB client
        :param collection_name:
            the JRS collection name to perform the lookup into
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._mongo_client = mongo_client
        self._collection = self._collection(collection_name)

    def _collection(self,
                    collection_name: str) -> CendantCollection:
        return CendantCollection(some_db_name='cendant',
                                 some_collection_name=collection_name,
                                 some_base_client=self._mongo_client,
                                 is_debug=self._is_debug)

    def find_by_job_role_id(self,
                            job_role_id: str) -> DataFrame:
        """
        Purpose:
            Construct a pandas DataFrame of results
        :param job_role_id:
            any Job Role ID
        :return:
            A JRS DataFrame that looks like this
            +-----+------------------------------------------------------------+-------------+-------------------------------------------------------+-----------+--------------------------------------------------+
            |     | JobRoleDescription                                         |   JobRoleId | JobRoleName                                           |   SkillId | SkillName                                        |
            |-----+------------------------------------------------------------+-------------+-------------------------------------------------------+-----------+--------------------------------------------------|
            |   0 | DO NOT SELECT - SCHEDULED FOR DELETION                     |      043846 | Software Release Manager: System z - Storage Software |    135032 | Perform Secure Engineering Readiness Assessments |
            |   1 | DO NOT SELECT - SCHEDULED FOR DELETION                     |      043846 | Software Release Manager: System z - Storage Software |    111693 | Perform Technical Team Leadership                |
            |   2 | DO NOT SELECT - SCHEDULED FOR DELETION                     |      043846 | Software Release Manager: System z - Storage Software |    135031 | Use Secure Methods/Tools & Components            |
            |   3 | DO NOT SELECT - SCHEDULED FOR DELETION                     |      043846 | Software Release Manager: System z - Storage Software |    063611 | Apply Project Management Methodologies           |
            +-----+------------------------------------------------------------+-------------+-------------------------------------------------------+-----------+--------------------------------------------------+
        """
        q = {"fields.value": job_role_id, "fields.name": "job_role_id"}
        results = self._collection.find_by_query(q)

        def _jrs_field(a_field_name: str) -> str:
            return [field for field in results[0]["fields"] if field["name"] == a_field_name][0]["value"]

        job_role_name = _jrs_field("job_role_name")
        job_role_description = _jrs_field("job_role_description")

        master = []
        for result in results:
            def _skill_field(a_field_name: str) -> str:
                return [field for field in result["fields"] if field["name"] == a_field_name][0]["value"]

            skill_id = _skill_field("skill_id")
            skill_name = _skill_field("skill_name")

            master.append({
                "JobRoleId": job_role_id,
                "JobRoleName": job_role_name,
                "JobRoleDescription": job_role_description,
                "SkillId": skill_id,
                "SkillName": skill_name})

        return pd.DataFrame(master)
