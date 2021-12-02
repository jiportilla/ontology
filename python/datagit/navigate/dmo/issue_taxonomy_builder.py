# !/usr/bin/env python
# -*- coding: UTF-8 -*-


import pandas as pd
from pandas import DataFrame
from tabulate import tabulate

from base import BaseObject


class IssueTaxonomyBuilder(BaseObject):
    """ Build a Taxonomy of records for a given GitHub Issue

    Sample Input:
        an index of GitHub records produced in the prior pipeline step
        {   '1111': { ... },
            '1111-15347705': { ... },
            '1111-15347724': { ... },
            ...
            '1111-66735318-MDY6Q29tbWl0NTA4MzYzOjAxODMyZmY3MTYzNDY3NDUyY2JlYWQzNTAyNTM4NDhhYTAwMWU4ZGM=-9': { ... } }

    Sample Output:
        +-----+-----------------------------------------------------------------------------------------------+-----------------+-------------+
        |     | ID                                                                                            | Parent          | Type        |
        |-----+-----------------------------------------------------------------------------------------------+-----------------+-------------|
        |   0 | 1111                                                                                          | 1111            | issue       |
        |   1 | 1111-63467879                                                                                 | 1111            | assignment  |
        |   2 | 1111-63708424                                                                                 | 1111            | mentions    |
        |   3 | 1111-63708425                                                                                 | 1111            | mentions    |
        |   4 | 1111-66059720                                                                                 | 1111            | mentions    |
        |   5 | 1111-66059720-MDY6Q29tbWl0NTA4MzYzOjkzZGU4NjUwZWUwY2YzZThmNGY3YmEyYzc3OWY2ZTYxNTlkNzM3NGU=    | 1111-66059720   | commit      |
        ..
        | 185 | 1111-15919326                                                                                 | 1111            | comment     |
        | 186 | 1111-15921104                                                                                 | 1111            | comment     |
        | 187 | 1111-15947755                                                                                 | 1111            | comment     |
        +-----+-----------------------------------------------------------------------------------------------+-----------------+-------------+

    Prereq:
        The index of GitHub records must exist
    """

    def __init__(self,
                 is_debug: bool = True):
        """
        Created:
            17-Dec-2019
            craig.trim@ibm.com
            *   refactored out of 'navigate-github-records'
                https://github.ibm.com/-cdo/unstructured-analytics/issues/1543#issuecomment-16682057
        Updated:
            25-Dec-2019
            craig.trim@ibm.com
            *   logging updates in pursuit of
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1651
        :param is_debug:
            if True     increase log output at DEBUG level
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug

    def _to_dataframe(self,
                      results: list) -> DataFrame:
        df = pd.DataFrame(results)

        if self._is_debug:
            if df.empty:
                self.logger.debug('\n'.join([
                    "Issue Taxonomy Building Complete",
                    f"\tNo Records Found"]))
            else:
                self.logger.debug('\n'.join([
                    "Issue Taxonomy Building Complete",
                    f"\tTotal Records: {len(df)}"]))

        return df

    def process(self,
                d_index: dict) -> DataFrame:
        results = []

        for key_field in d_index:
            record = d_index[key_field]
            record_type = record['div_field']
            parent_key_field = record['key_field_parent']

            results.append({
                "ID": key_field,
                "Parent": parent_key_field,
                "Type": record_type})

        df = self._to_dataframe(results)

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Index Taxonomy Completed",
                tabulate(df, tablefmt='psql', headers='keys')]))

        return df
