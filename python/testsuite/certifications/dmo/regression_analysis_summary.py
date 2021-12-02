# !/usr/bin/env python
# -*- coding: UTF-8 -*-


import pandas as pd
from pandas import DataFrame

from base import BaseObject


class RegressionAnalysisSummary(BaseObject):
    """ An analysis of the regression results will produce two DataFrames:
        1.  Gold Regression Analysis
        2.  Standard Regression Analysis

        The Summarized DataFrame may be segmented by Vendor like this:
            +----+----------+--------------------+------------------+---------------+---------+----------+
            |    |   Failed | RegressionSuite    | RegressionType   |   SuccessRate |   Total | Vendor   |
            |----+----------+--------------------+------------------+---------------+---------+----------|
            |  0 |        3 | self_certification | Gold             |          50   |       6 | SAP      |
            |  1 |        1 | self_certification | Gold             |          66.7 |       3 | Redhat   |
            |  2 |       12 | self_certification | Standard         |          55.6 |      27 | SAP      |
            |  3 |        6 | self_certification | Standard         |          45.5 |      11 | Redhat   |
            +----+----------+--------------------+------------------+---------------+---------+----------+

        Or just fully summarized across the board:
            +----+----------+--------------------+------------------+---------------+---------+
            |    |   Failed | RegressionSuite    | RegressionType   |   SuccessRate |   Total |
            |----+----------+--------------------+------------------+---------------+---------|
            |  0 |        4 | self_certification | Gold             |          55.6 |       9 |
            |  1 |       18 | self_certification | Standard         |          52.6 |      38 |
            +----+----------+--------------------+------------------+---------------+---------+

        In either event, we know at a glance how the Gold Regression fared and how the Standard Regression fared
        The vendor-segmented view helps shows where immediate action should be focused
            e.g., in the example above, the Gold Standard for SAP is doing poorly
    """
    __df_summary = None

    def __init__(self,
                 regression_suite_name: str,
                 df_gold_analysis: DataFrame,
                 df_standard_analysis: DataFrame,
                 segment_by_vendor: bool,
                 is_debug: bool = False):
        """
        Created:
            12-Aug-2019
            craig.trim@ibm.com
            *   refactored out of 'run-regression-suite' in pursuit of
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/717
        :param regression_suite_name:
            the name of the regression suite this is for (e.g., 'synonyms' or 'self-certification')
        :param segment_by_vendor:
            True        segment (summarize) the DataFrames by Vendor
            False       summarize across the board
        :param df_gold_analysis:
            the Gold Analysis regression results
        :param df_standard_analysis:
            the Standard Analysis regression results
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._process(regression_suite_name=regression_suite_name,
                      segment_by_vendor=segment_by_vendor,
                      df_gold_analysis=df_gold_analysis,
                      df_standard_analysis=df_standard_analysis)

    def summary(self) -> DataFrame:
        return self.__df_summary

    @staticmethod
    def _accuracy(total_records: float,
                  total_failed_records: float) -> float:
        if total_failed_records == 0:
            return float(100.0)
        if total_failed_records == total_records:
            return float(0.0)

        f_total = float(total_records)
        f_fails = float(total_failed_records)

        x = float((f_total - f_fails) / f_total) * 100.0
        return round(x, ndigits=1)

    def _summarize_by_vendor(self,
                             regression_type: str,
                             regression_suite_name: str,
                             df_analysis: DataFrame) -> list:
        """
        Purpose:
            Perform Summarization (Segmentation) by Vendor
        :param regression_type:
            the type of regression analysis being summarized (e.g., Gold or Standard)
        :param regression_suite_name:
            the name of the regression suite (e.g., self_certification)
        :param df_analysis:
            the analysis dataframe (e.g., Gold Regression Analysis)
        :return:
            a list of results
        """

        results = []
        for vendor in df_analysis['Vendor'].unique():
            print ("VENDOR ---> ",vendor)
            df_vendor = df_analysis[df_analysis['Vendor'] == vendor]

            total = 0
            failed = 0
            for _, row in df_vendor.iterrows():
                total += row["Total"]
                failed += row["Failed"]

            if total == 0:
                continue

            success_rate = self._accuracy(total_records=total,
                                          total_failed_records=failed)
            results.append({
                "Vendor": vendor,
                "Total": total,
                "Failed": failed,
                "SuccessRate": success_rate,
                "RegressionType": regression_type,
                "RegressionSuite": regression_suite_name})

        return results

    @staticmethod
    def _summarize(regression_type: str,
                   regression_suite_name: str,
                   df_analysis: DataFrame) -> list:
        """
        Purpose:
            Perform Summarization
        :param regression_type:
            the type of regression analysis being summarized (e.g., Gold or Standard)
        :param regression_suite_name:
            the name of the regression suite (e.g., self_certification)
        :param df_analysis:
            the analysis dataframe (e.g., Gold Regression Analysis)
        :return:
            a list of results
        """
        results = []

        for _, row in df_analysis[df_analysis['Result'] == 'Summary'].iterrows():
            results.append({
                "Total": row["Total"],
                "Failed": row["Failed"],
                "SuccessRate": row["SuccessRate"],
                "RegressionType": regression_type,
                "RegressionSuite": regression_suite_name})

        return results

    def _process(self,
                 regression_suite_name: str,
                 df_gold_analysis: DataFrame,
                 df_standard_analysis: DataFrame,
                 segment_by_vendor: bool) -> None:
        """
        Purpose:
            Summarize the Regression Analysis DataFrames
        :param regression_suite_name:
            the name of the regression suite (e.g., 'self_certifications')
        :param df_gold_analysis:
            the Gold Regression Analysis results
        :param df_standard_analysis:
            the Standard Regression Analysis results
        :param segment_by_vendor:
            True        segment (summarize) the DataFrames by Vendor
            False       summarize across the board
        :return:
            a summarized DataFrame
        """
        results = []

        if segment_by_vendor:
            results += self._summarize_by_vendor(regression_type="Gold",
                                                 regression_suite_name=regression_suite_name,
                                                 df_analysis=df_gold_analysis)
            results += self._summarize_by_vendor(regression_type="Standard",
                                                 regression_suite_name=regression_suite_name,
                                                 df_analysis=df_standard_analysis)
        else:
            results += self._summarize(regression_type="Gold",
                                       regression_suite_name=regression_suite_name,
                                       df_analysis=df_gold_analysis)
            results += self._summarize(regression_type="Standard",
                                       regression_suite_name=regression_suite_name,
                                       df_analysis=df_standard_analysis)

        self.__df_summary = pd.DataFrame(results)
