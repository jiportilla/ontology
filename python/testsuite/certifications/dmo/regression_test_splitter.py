# !/usr/bin/env python
# -*- coding: UTF-8 -*-


import pandas as pd
from pandas import DataFrame

from base import BaseObject


class RegressionTestSplitter(BaseObject):
    """ Divide Regression results into "Gold Regression" vs "Standard Regression"

        The Regression CSV file is here
        https://github.ibm.com/GTS-CDO/unstructured-analytics/blob/master/resources/testing/regressions/certifications.csv
            (relative path - resources/testing/regressions/certifications.csv)

        The Gold Regression will contain tests
            that MUST PASS AT 100%,
            or the build will halt.

        The Standard regression will contain tests
            that must maintain a level of accuracy (typically 80-85%)
            or the team will determine whether to halt the build.
    """

    __df_gold = None
    __df_standard = None

    def __init__(self,
                 df_results: DataFrame,
                 is_debug: bool = False):
        """
        Created:
            12-Aug-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/711#issuecomment-13813324
        :param df_results:
            the incoming regression test results
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._process(df_results)

    def results(self) -> (DataFrame, DataFrame):
        return self.__df_gold, self.__df_standard

    def _process(self,
                 df_results: DataFrame) -> None:
        """
        Purpose:
            Split a Regression Test into Gold vs Standard
        Criteria:
            If the Input Text and Expected Result are identical,
                append the test to the gold regression
        :param df_results:
            the incoming regression test results
        """
        gold_tests = []
        standard_tests = []

        for _, row in df_results.iterrows():
            expected_result = row["ExpectedResult"]
            input_text = row["InputText"]

            def _generate_row():
                return {
                    "Vendor": row["Vendor"],
                    "ActualResults": row["ActualResults"],
                    "NormalizedText": row["NormalizedText"],
                    "ExpectedResult": expected_result,
                    "InputText": input_text,
                    "Pass": row["Pass"]}

            def _is_equal() -> bool:
                x = input_text.replace('"', '').lower().strip()
                y = expected_result.replace('"', '').lower().strip()
                return x == y

            if _is_equal():
                gold_tests.append(_generate_row())
            else:
                standard_tests.append(_generate_row())

        self.__df_gold = pd.DataFrame(gold_tests)
        self.__df_standard = pd.DataFrame(standard_tests)
