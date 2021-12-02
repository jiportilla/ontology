#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pprint
from collections import Counter

import pandas as pd
from pandas import DataFrame
from tabulate import tabulate

from base import BaseObject
from datadict import FindDimensions


class AcademicDimensionCalculator(BaseObject):
    """ Compute the Academic Dimension """

    __d_score_mapping = {
        "Associates": 1.0,
        "Bachelors": 2.0,
        "Masters": 4.0,
        "PhD": 8.0}

    def __init__(self,
                 source_record: dict,
                 is_debug: bool = False):
        """
        Created:
            1-Nov-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1151#issuecomment-15692427
        Updated:
            15-Nov-2019
            craig.trim@ibm.com
            *   add 'transform-field'
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1373
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._source_record = source_record
        self._dim_finder = FindDimensions(schema="degrees")

        # state
        self._df_result = None
        self._d_degrees = None

    def result(self):
        class Facade(object):
            @staticmethod
            def degrees() -> dict:
                return self._d_degrees

            @staticmethod
            def final() -> DataFrame:
                return self._df_result

        return Facade()

    @staticmethod
    def _transform_field(a_field: dict):  # GIT-1373
        """
        Purpose:
            Transform Common Abbreviations to Degree Names
        Notes:
        -   this is handled in GIT-1373-16032810
            but will not be present in any tag collection <= supply_tag_20191114
        -   please retain this function for backward compatibility
        :param a_field:
            any degree-name field
        :return:
            a (potentially modified) degree name field
        """
        if a_field['value'] == ['BA']:
            a_field['tags']['supervised'].append(('Bachelor of Arts', 95))
        if a_field['value'] == ['BS']:
            a_field['tags']['supervised'].append(('Bachelor of Science', 95))
        elif a_field['value'] == ['MA']:
            a_field['tags']['supervised'].append(('Master of Arts', 95))
        elif a_field['value'] == ['MS']:
            a_field['tags']['supervised'].append(('Master of Science', 95))
        return a_field

    def _degree_fields(self) -> list:
        matching_fields = []
        for field in self._source_record["fields"]:
            if field["name"] == "degree_name":
                field = self._transform_field(field)
                matching_fields.append(field)
        return matching_fields

    @staticmethod
    def _tags(matching_fields: list) -> list:
        s = set()
        for field in matching_fields:
            if "tags" in field:
                [s.add(x[0]) for x in field["tags"]["supervised"]]
        return sorted(s)

    def _apply_schema(self,
                      tags: list) -> dict:
        c = Counter()
        for tag in tags:
            for result in self._dim_finder.find(tag):
                c.update({result: 1})

        return dict(c)

    def _score_mapping(self,
                       schema_element: str) -> float:
        for k in self.__d_score_mapping:
            if k == schema_element:
                return self.__d_score_mapping[k]
        return 0.0

    def _compute_dataframe(self,
                           score: float,
                           weight_multiplier: float = 15.0):
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
        self._df_result = pd.DataFrame([{"Schema": "academic",
                                         "Weight": score * weight_multiplier,
                                         "zScore": score,
                                         "zScoreNorm": score}])

        if self._is_debug:
            self.logger.debug('\n'.join([
                f"Compute Academic Dimension",
                f"\tScore: {score}",
                f"\tKey Field: {self._source_record['key_field']}",
                tabulate(self._df_result,
                         tablefmt='psql',
                         headers='keys')]))

    def _score(self) -> float:
        scores = []

        for k in self._d_degrees:
            scores.append(self._score_mapping(k))

        if len(scores):
            return max(scores)

        return 0.0

    def process(self):
        degree_fields = self._degree_fields()

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Located Degrees",
                f"\tKey Field: {self._source_record['key_field']}",
                pprint.pformat(degree_fields)]))

        self._d_degrees = self._apply_schema(
            self._tags(degree_fields))

        self._compute_dataframe(self._score())

        return self.result()
