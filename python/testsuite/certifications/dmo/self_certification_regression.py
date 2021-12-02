# !/usr/bin/env python
# -*- coding: UTF-8 -*-


import pandas as pd
from pandas import DataFrame
from tabulate import tabulate

from base import BaseObject
from nlusvc import TextAPI


class SelfCertificationRegression(BaseObject):
    """ Component Logic for the Regression Test on Self-Reported Certifications
    """

    __text_api = None

    def __init__(self,
                 df_regression: DataFrame,
                 is_debug: bool = False):
        """
        Created:
            9-Aug-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/683
        """
        BaseObject.__init__(self, __name__)

        self._results = []
        self._is_debug = is_debug
        self._df_regression = df_regression

    def _annotate(self,
                  input_text: str) -> (str, list):
        """
        Purpose:
            Annotate Input Text
        :param input_text:
            any input text
        :return:
            a list of 0..* annotations (tags)
        """
        if not self.__text_api:
            self.__text_api = TextAPI()

        df_results = self.__text_api.parse(input_text)

        if self._is_debug:
            self.logger.debug(tabulate(df_results,
                                       headers='keys',
                                       tablefmt='psql'))
        if df_results.empty:
            if self._is_debug:
                self.logger.debug('\n'.join([
                    "Empty DataFrame",
                    f"\tInput Text: {input_text}"]))
            return None, []

        normalized_text = df_results['NormalizedText'].unique()[0]
        tags = sorted(set(df_results['Tag'].unique()))

        return normalized_text, tags

    def _regression_by_key(self,
                           some_key: str) -> DataFrame:
        """
        Purpose:
            Run the Regression by Key
        :param some_key:
            a key into the regression DataFrame
            e.g., key='ABAP for SAP HANA 2.0' would run the regression
            on all the variations for this certification
        :return:
            the DataFrame for this key
        """

        df_key = self._df_regression[self._df_regression['ExpectedResult'] == some_key]

        def _vendor():
            vendors = df_key['Vendor'].unique()
            if vendors:
                return vendors[0]

        vendor = _vendor()
        for input_text in df_key['Input'].unique():

            normalized_text, tags = self._annotate(input_text)
            if not normalized_text or not tags:
                continue

            has_key = some_key in tags

            self._results.append({
                "Vendor": vendor,
                "ExpectedResult": some_key,
                "ActualResults": ', '.join(tags),
                "Pass": has_key,
                "InputText": input_text,
                "NormalizedText": normalized_text})

        return df_key

    def process(self):
        keys = [x.strip() for x in
                self._df_regression['ExpectedResult'].unique()]

        ctr = 0
        total_keys = len(keys)

        for key in keys:
            ctr += 1

            df_key = self._regression_by_key(key)

            if self._is_debug:
                self.logger.debug(f"Regression Testing "
                                  f"(key='{key}', "
                                  f"total={len(df_key)}, "
                                  f"progress={ctr}-{total_keys})")

        return pd.DataFrame(self._results)
