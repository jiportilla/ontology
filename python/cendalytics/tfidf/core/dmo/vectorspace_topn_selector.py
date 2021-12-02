#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pandas as pd
from pandas import DataFrame
from tabulate import tabulate

from base import BaseObject


class VectorSpaceTopNSelector(BaseObject):
    """
    Purpose:
        Read the Vector Space and return the most discriminating terms (top-n)
            by key-field
    Sample Output:
        +----+--------+------------+
        |    |   Rank | Term       |
        |----+--------+------------|
        |  0 |      1 | windows nt |
        |  1 |      2 | rfs        |
        |  2 |      3 | microsoft  |
        +----+--------+------------+
    """

    __df = None

    def __init__(self,
                 df: DataFrame,
                 is_debug: bool = False):
        """
        Created:
            7-Nov-2019
            craig.trim@ibm.com
            *   refactored out of 'read-collection-vectorspace'
        :param df:
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)

        self._df = df
        self._is_debug = is_debug

    def _filter_df(self,
                   serial_number: str) -> DataFrame:
        """
        Purpose:
            Filter a DataFrame by Serial Number
        :param serial_number:
            a serial number (CNUM)
        :return:
            a DataFrame
        """

        # Step: Filter DataFrame by CNUM (e.g., 'Doc')
        df2 = self._df[self._df['Doc'] == serial_number]

        if df2.empty:  # No Results Found
            self.logger.warning("\n".join([
                "Serial Number Not Found (cnum={})".format(
                    serial_number)]))

        elif self._is_debug:  # Show Results by CNUM
            self.logger.debug("\n".join([
                "Filtered DataFrame (cnum={})".format(serial_number),
                tabulate(df2,
                         headers='keys',
                         tablefmt='psql')]))

        return df2  # filtered DataFrame

    @staticmethod
    def _top_n_skills(df2: DataFrame,
                      top_n: int = 3):

        df2 = df2.sort_values(by=['TFIDF'], ascending=False)
        tags = df2['Term'].unique()

        if top_n < len(tags):
            return tags[:top_n]
        return tags

    @staticmethod
    def _to_df(tags: list) -> DataFrame:
        ctr = 1
        results = []
        for tag in tags:
            results.append({
                "Rank": ctr,
                "Term": tag})
            ctr += 1
        return pd.DataFrame(results)

    @staticmethod
    def _to_expanded_df(top_tags: list,
                        all_tags: list) -> DataFrame:

        def _partial_matches(a_token: str) -> set:
            partial_matches = set()
            for a_tag in all_tags:
                if a_token in a_tag.lower():
                    if a_tag != a_token and a_tag not in top_tags:
                        partial_matches.add(a_tag)
            return partial_matches

        d_expansions = {}
        for top_tag in top_tags:
            matches = set()
            tokens = top_tag.lower().split(' ')
            for token in tokens:
                matches = matches.union(_partial_matches(token))
            d_expansions[top_tag] = sorted(matches)

        ctr = 1
        results = []
        for k in d_expansions:
            if len(d_expansions[k]):
                for expansion in d_expansions[k]:
                    results.append({
                        "Rank": ctr,
                        "Term": k,
                        "Expansion": expansion})

            ctr += 1

        return pd.DataFrame(results)

    def process(self,
                key_field: str,
                expand: bool = False,
                top_n: int = 3) -> DataFrame or None:
        """
        Purpose:
        :param expand:
        :param key_field:
            the key field to search the Vector Space on
            Key Fields by Collection:
                supply_tag_<date>       'Serial Number'
                demand_tag_<date>       'Open Seat ID'
                learning_tag_<date>     'LAID'
        :param top_n:
            the number of tags to search for
        :return:
            a pandas DataFrame of results
            Sample Output:
                +----+--------+------------+
                |    |   Rank | Term       |
                |----+--------+------------|
                |  0 |      1 | windows nt |
                |  1 |      2 | rfs        |
                |  2 |      3 | microsoft  |
                +----+--------+------------+
        """
        df2 = self._filter_df(key_field)
        if df2.empty:
            return None

        all_tags = df2['Term'].unique()
        top_tags = self._top_n_skills(df2, top_n)

        if not expand:
            return self._to_df(top_tags)

        return self._to_expanded_df(top_tags=top_tags,
                                    all_tags=all_tags)
