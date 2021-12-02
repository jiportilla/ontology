# !/usr/bin/env python
# -*- coding: UTF-8 -*-


import pandas as pd
from pandas import DataFrame

from base import BaseObject


class AnalyzeRegressionSuite(BaseObject):
    __df_gold_analysis = None
    __df_standard_analysis = None

    def __init__(self,
                 df_results: DataFrame,
                 is_debug: bool = False):
        """
        Created:
            12-Aug-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/680
        :param df_results:
            the regression test results
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._process(df_results)

    def results(self) -> (DataFrame, DataFrame):
        return self.__df_gold_analysis, self.__df_standard_analysis

    def _process(self,
                 df_results: DataFrame) -> None:
        """
        Purpose:
            Split the Regression Results into
                Gold vs. Standard
            and perform a summarized analysis on each
        :param df_results:
            the regression test results
        """
        from testsuite.certifications.dmo import RegressionTestSplitter
        from testsuite.certifications.dmo import RegressionResultAnalysis

        df_gold, df_standard = RegressionTestSplitter(df_results).results()

        def analyze_gold_regression():
            if df_gold.empty:
                self.logger.warning("Gold Regression is empty")
                return pd.DataFrame([{
                    "Result": None,
                    "Vendor": None,
                    "Total": 0,
                    "Failed": 0,
                    "SuccessRate": 0}])
            return RegressionResultAnalysis(df_gold,
                                            is_debug=self._is_debug).results()

        def analyze_standard_regression():
            if df_standard.empty:
                self.logger.warning("Standard Regression is empty")
                return pd.DataFrame([{
                    "Result": None,
                    "Vendor": None,
                    "Total": 0,
                    "Failed": 0,
                    "SuccessRate": 0}])
            return RegressionResultAnalysis(df_standard,
                                            is_debug=self._is_debug).results()

        self.__df_gold_analysis = analyze_gold_regression()
        self.__df_standard_analysis = analyze_standard_regression()
