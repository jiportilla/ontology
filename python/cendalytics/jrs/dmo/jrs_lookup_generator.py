#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pandas as pd
from pandas import DataFrame
from tabulate import tabulate

from base import BaseObject
from datadict import FindRelationships
from datamongo import BaseMongoClient
from datamongo import CendantCollection
from nlutext import TextParser


class JrsLookupGenerator(BaseObject):
    """
    Purpose:
    """

    def __init__(self,
                 host_name: str = None,         # deprecated/ignored
                 is_debug: bool = False):
        """
        Created:
            24-Sept-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/-cdo/unstructured-analytics/issues/992
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._text_parser = TextParser(is_debug=is_debug)
        self._rel_finder = FindRelationships(is_debug=is_debug)

    def _records(self) -> list:
        collection = CendantCollection(some_db_name='cendant',
                                       some_collection_name='ingest_jrs',
                                       some_base_client=BaseMongoClient(),
                                       is_debug=self._is_debug)
        records = collection.all()
        if self._is_debug:
            self.logger.debug(f"Retrieved JRS Records: "
                              f"(total={len(records)})")

        return records

    def is_skill(self,
                 some_token: str) -> bool:
        return self._rel_finder.has_any_ancestors(some_token, ['Role', 'Skill'])

    @staticmethod
    def _to_dataframe(results: dict) -> DataFrame:
        _results = []
        for job_role_id in results:
            for skill_set_id in results[job_role_id]:
                for result in results[job_role_id][skill_set_id]:
                    for tag in result["tags"]:
                        _results.append({
                            "JobRoleId": job_role_id,
                            "SkillSetId": skill_set_id,
                            "JobRoleName": result["job_role_name"],
                            "SkillSetName": result["skill_set_name"],
                            "JRSS": result["jrss"],
                            "CendantSkill": tag})

        return pd.DataFrame(_results)

    def process(self,
                limit: int = None) -> DataFrame:
        """
        Purpose:

        :return:
            +----+------------------------+-----------------------------------------------------------------------+-------------+------------------------------------+--------------+-------------------------------------------------+
            |    | CendantSkill           | JRSS                                                                  |   JobRoleId | JobRoleName                        | SkillSetId   | SkillSetName                                    |
            |----+------------------------+-----------------------------------------------------------------------+-------------+------------------------------------+--------------+-------------------------------------------------|
            |  0 | Database Administrator | Application Database Administrator-Physical Security Services         |      040685 | Application Database Administrator | S1866        | Physical Security Services                      |
            |  1 | Application Developer  | Application Developer-Brokerage Svcs Integration, Adapters & Prod Ext |      040684 | Application Developer              | S9314        | Brokerage Svcs Integration, Adapters & Prod Ext |
            |  2 | Application Developer  | Application Developer-GTS Analytics                                   |      040684 | Application Developer              | S7687        | GTS Analytics                                   |
            |  3 | Application Developer  | Application Developer-GTS Labs-Automation & Cognitive Delivery        |      040684 | Application Developer              | S7387        | GTS Labs-Automation & Cognitive Delivery        |
            |  4 | Cognitive Skill        | Application Developer-GTS Labs-Automation & Cognitive Delivery        |      040684 | Application Developer              | S7387        | GTS Labs-Automation & Cognitive Delivery        |
            |  5 | Application Developer  | Application Developer-GTS Labs-Brokerage Product Development          |      040684 | Application Developer              | S9313        | GTS Labs-Brokerage Product Development          |
            +----+------------------------+-----------------------------------------------------------------------+-------------+------------------------------------+--------------+-------------------------------------------------+
        """
        records = self._records()

        tags = set()
        results = {}

        ctr = 0
        for record in records:

            ctr += 1
            if ctr % 10 == 0:
                self.logger.debug(f"Processing {ctr}-{len(records)}")

            def _by_field_name(a_name: str) -> str:
                _fields = [field for field in record["fields"] if field["name"] == a_name]
                return _fields[0]["value"]

            job_role_jrss = _by_field_name("jrss")
            job_role_name = _by_field_name("job_role")
            job_role_id = _by_field_name("job_role_id")
            skill_set_id = _by_field_name("skill_set_id")
            skill_set_name = _by_field_name("skill_set")

            df_result = self._text_parser.process(original_ups=job_role_jrss,
                                                  use_profiler=self._is_debug,
                                                  as_dataframe=True)

            if self._is_debug:
                self.logger.debug('\n'.join([
                    f"Generated Result (field-value={job_role_jrss})",
                    tabulate(df_result,
                             headers='keys',
                             tablefmt='psql')]))

            tags = [tag for tag in df_result.Tag.unique()
                    if self.is_skill(tag)]

            if job_role_id not in results:
                results[job_role_id] = {}
            if skill_set_id not in results[job_role_id]:
                results[job_role_id][skill_set_id] = []
            results[job_role_id][skill_set_id].append({
                "job_role_name": job_role_name,
                "skill_set_name": skill_set_name,
                "jrss": job_role_jrss,
                "tags": tags})

            if limit and ctr > limit:
                break

        return self._to_dataframe(results)
