#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pandas as pd
from pandas import DataFrame

from base import BaseObject


class GenerateMetaSentiment(BaseObject):
    """  Retrieve Source Records for Feedback Sentiment Processing """

    def __init__(self,
                 df_summary: DataFrame,
                 is_debug: bool = False):
        """
        Created:
            16-Jan-2020
            craig.trim@ibm.com
            *   the refactoring of a notebook from
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1746
        :param df_summary:
            DataFrame of this Report:
                'feedback_tag_<DATE>-summary-<TS>.csv'
            e.g.,
                'feedback_tag_20191202-summary-1575690754.csv'
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._df_summary = df_summary

    def _summarize(self) -> DataFrame:

        record_ids = sorted(self._df_summary['RecordID'].unique())

        master = []

        for record_id in record_ids:
            df2 = self._df_summary[self._df_summary['RecordID'] == record_id]

            def _region():
                country = df2['Country'].unique()[0]
                region = df2['Region'].unique()[0]
                if region.lower() == 'africa':
                    return "mea"
                if region.lower() == 'middle east':
                    return "mea"
                if region.lower() == 'asia':
                    return "ap"
                if country.lower() in ["australia", "new zealand", "sri lanka", "india"]:
                    return "ap"
                if country.lower() in ["china", "hong kong", "taiwan"]:
                    return "gcg"
                return region

            cons = len(df2[df2['Category'] == 'Cons'])
            pros = len(df2[df2['Category'] == 'Pros'])
            suggestions = len(df2[df2['Category'] == 'Suggestions'])

            def adjudicate():
                if cons >= pros - 1 and cons > suggestions:
                    return "Cons"
                if pros > cons and pros > suggestions + 1:
                    return "Pros"
                return "Suggestions"

            for i in range(0, 10):
                master.append({
                    "Category": adjudicate(),
                    "Country": df2['Country'].unique()[0],
                    "Leadership": df2['Leadership'].unique()[0],
                    "RecordID": df2['RecordID'].unique()[0],
                    "Region": _region(),
                    "Schema": df2['Schema'].unique()[0],
                    "Tag": df2['Tag'].unique()[0],
                    "Tenure": df2['Tenure'].unique()[0]})

        df_output = pd.DataFrame(master)

        return df_output

    def process(self) -> DataFrame:
        return self._summarize()
