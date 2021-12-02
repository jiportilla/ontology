#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pandas as pd
from pandas import DataFrame
from tabulate import tabulate

from base import BaseObject


class TimeDimensionCalculator(BaseObject):
    """ Compute the Time and Experience Dimension """

    __FIRST_START_YEAR_MEAN = 2004.1850136788375
    __FIRST_START_YEAR_STDEV = 9.855146785665623

    def __init__(self,
                 source_record: dict,
                 is_debug: bool = False):
        """
        Created:
            21-Oct-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/591#issuecomment-15439418
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._source_record = source_record

        # state
        self._d_years = None
        self._df_result = None
        self._start_year = None

    def result(self):
        class Facade(object):
            @staticmethod
            def career_history() -> list:
                return self._d_years

            @staticmethod
            def start_year() -> list:
                return self._start_year

            @staticmethod
            def final() -> DataFrame:
                return self._df_result

        return Facade()

    def _career_history(self) -> list:
        fields = []
        for field in self._source_record["fields"]:

            # structure type 1 (pre 2019-10-15)
            if type(field["collection"]) == str:
                if field["collection"].startswith("ingest_cv_career_history"):
                    fields.append(field)

            # structure type 2 (post 2019-10-15)
            elif type(field["collection"]) == dict:
                if field["collection"]["type"] == "ingest_cv_career_history":  # modern structure
                    fields.append(field)

        return fields

    def _parse_years(self,
                     fields: list) -> None:
        """
        Purpose:
        :return:
            the minimum value or 0.0 if no records present
        """
        curr = {}
        years = []

        for field in fields:
            if field["name"] == "position_start_year":
                curr["start"] = int(field["value"][0])
            elif field["name"] == "position_end_year":
                curr["end"] = int(field["value"][0])
                years.append(curr)
                curr = {}
        if len(curr):
            years.append(curr)

        self._d_years = years

    @staticmethod
    def _zscore(a_value: float,
                a_mean: float,
                a_stdev: float):
        return round((a_value / a_mean) / a_stdev, 2)

    def _compute_dataframe(self,
                           score: float,
                           weight_multiplier: float = 15.0) -> None:
        """
        Purpose:
            Transform output into an xdm-compatible DataFrame
        Sample Output:
            +----+----------+----------+----------+--------------+
            |    | Schema   |   Weight |   zScore |   zScoreNorm |
            |----+----------+----------+----------+--------------|
            |  0 | academic |       60 |        4 |            4 |
            +----+----------+----------+----------+--------------+
        :param score:
            a computed score
        :param weight_multiplier:
            arbitrary value to keep the weight inflated above the z-score
            e.g.,   a value with a z-score of 4 often has
                    an underlying weight of ~60
            maintaining an approxiate ratio between z-score and weight
                is useful in polar projections and other visualizations
        """
        self._df_result = pd.DataFrame([{"Schema": "experience",
                                         "Weight": score * weight_multiplier,
                                         "zScore": score,
                                         "zScoreNorm": score}])
        if self._is_debug:
            self.logger.debug('\n'.join([
                f"Compute Time Dimension",
                f"\tScore: {score}",
                f"\tKey Field: {self._source_record['key_field']}",
                tabulate(self._df_result,
                         tablefmt='psql',
                         headers='keys')]))

    def _get_start_year(self) -> float:
        if len(self._d_years):
            return min([x["start"] for x in self._d_years])

        return 0.0

    def _compute_score(self) -> float:
        self._start_year = self._get_start_year()
        if self._start_year == 0.0:
            return 0.0

        z_first_start_year = self._zscore(a_value=self._start_year,
                                          a_mean=self.__FIRST_START_YEAR_MEAN,
                                          a_stdev=self.__FIRST_START_YEAR_STDEV)

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Computed Experience",
                f"\tFirst Start Year ({self._start_year}, "
                f"z={z_first_start_year})"]))

        return z_first_start_year

    def process(self):
        self._parse_years(self._career_history())

        self._compute_dataframe(self._compute_score())

        return self.result()
