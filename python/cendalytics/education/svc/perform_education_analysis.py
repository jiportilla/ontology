#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pandas as pd
from pandas import DataFrame

from base import BaseObject
from base import RecordUnavailableRecord
from cendalytics.education.dmo import EducationFieldCounter
from datamongo.core.bp import CendantCollection
from datamongo.core.dmo import BaseMongoClient


class PerformEducationAnalysis(BaseObject):
    """
    """

    __query = {"fields.collection.type": "ingest_cv_education"}

    def __init__(self,
                 collection_name: str,
                 some_base_client: BaseMongoClient = None,
                 limit: int = None,
                 is_debug: bool = False):
        """
        Created:
            17-Oct-2019
            craig.trim@ibm.com
            *   refactored out of education-analysis-api
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1142
        """
        BaseObject.__init__(self, __name__)

        if not some_base_client:
            some_base_client = BaseMongoClient()

        self._limit = limit
        self._is_debug = is_debug
        self._mongo_client = some_base_client
        self._collection = CendantCollection(some_base_client=self._mongo_client,
                                             some_collection_name=collection_name,
                                             is_debug=self._is_debug)

    def _records(self,
                 field_name: str) -> list:
        """
        Purpose:
            Retrieve Records by <Query>
        :param field_name:
            the field name to filter the query on
        :return:
            a list of valid records 1..*
        """
        query = self.__query
        query["fields.name"] = field_name

        records = self._collection.find_by_query(some_query=self.__query,
                                                 limit=self._limit)
        if not records or not len(records):
            self.logger.warning('\n'.join([
                "No Records Found",
                f"\tQuery: {self.__query}",
                f"\tCollection: {self._collection.collection_name}"]))
            raise RecordUnavailableRecord

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Records Retrieved",
                f"\tQuery: {self.__query}",
                f"\tTotal Records: {len(records)}",
                f"\tCollection: {self._collection.collection_name}"]))

        return records

    def _by_field_name(self,
                       field_name: str,
                       preprocess: bool,
                       normalize: bool) -> DataFrame:
        records = self._records(field_name)
        dmo = EducationFieldCounter(records=records,
                                    field_name=field_name,
                                    is_debug=self._is_debug)

        c = dmo.process(preprocess, normalize)

        results = []
        for item in c:
            results.append({"Item": item, "Frequency": c[item]})

        df = pd.DataFrame(results)
        return df.sort_values(by=['Frequency'], ascending=False)

    def majors(self,
               preprocess: bool = False,
               normalize: bool = False) -> DataFrame:
        """
        Purpose:
            Generate a DataFrame with a Frequency of Majors
        Sample Output:
            +-----+-------------+----------------------------------------------------------------------------------------------+
            |     |   Frequency | Item                                                                                         |
            |-----+-------------+----------------------------------------------------------------------------------------------|
            |   1 |          47 | unknown                                                                                      |
            |  14 |          44 | computer_science                                                                             |
            |  55 |          29 | pc                                                                                           |
            |   9 |          27 | information_technology                                                                       |
            |  21 |          27 | electrical_engineering                                                                       |
            |   2 |          22 | electronics and communication                                                                |
            |  48 |          20 | computer_engineering                                                                         |
            ...
            | 242 |           1 | state board                                                                                  |
            +-----+-------------+----------------------------------------------------------------------------------------------+
        :param preprocess:
            True        perform textual pre-processing prior to counting
                        uses Textacy preprocessing
        :param normalize:
            True        perform textual normalization prior to counting
                        uses Cendant normalization pipeline
        :return:
            a DataFrame of results
        """
        return self._by_field_name(field_name="major",
                                   preprocess=preprocess,
                                   normalize=normalize)

    def degrees(self,
                preprocess: bool = True,
                normalize: bool = True) -> DataFrame:
        """
        Purpose:
            Generate a DataFrame with a Frequency of Degrees
        Sample Output:
            +-----+-------------+---------------------------------------------------------------------------+
            |     |   Frequency | Item                                                                      |
            |-----+-------------+---------------------------------------------------------------------------|
            |   2 |         142 | other                                                                     |
            |   0 |          90 | bachelor_of_technology                                                    |
            |   1 |          44 | bachelor_of_engineering                                                   |
            |  16 |          25 | bachelor_of_science                                                       |
            |  12 |          24 | bachelor_degree                                                           |
            |  33 |          22 | bachelor_of_business                                                      |
            |  41 |          17 | master_of_engineering                                                     |
            ...
            | 177 |           1 | bachelor_of_technology with computer_science                              |
            +-----+-------------+---------------------------------------------------------------------------+
        :param preprocess:
            True        perform textual pre-processing prior to counting
                        uses Textacy preprocessing
        :param normalize:
            True        perform textual normalization prior to counting
                        uses Cendant normalization pipeline
        :return:
            a DataFrame of results
        """

        return self._by_field_name(field_name="degree_name",
                                   preprocess=preprocess,
                                   normalize=normalize)
