# !/usr/bin/env python
# -*- coding: UTF-8 -*-


import pandas as pd
from pandas import DataFrame

from base import BaseObject
from nlusvc import TextAPI


class SynonymsRegression(BaseObject):
    """ Component Logic for the Regression Test on Synonyms
    """

    __text_api = None

    def __init__(self,
                 df_regression: DataFrame,
                 is_debug: bool = False):
        """
        Created:
            10-Aug-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/683
        """
        BaseObject.__init__(self, __name__)

        self._is_debug = is_debug
        self._df_regression = df_regression

    def _normalize(self,
                   input_text: str) -> str:
        """
        Purpose:
            Normalize Input Text
        :param input_text:
            any input text
        :return:
            a normalize output string
        """
        if not self.__text_api:
            self.__text_api = TextAPI()

        normalized_text = self.__text_api.normalize(input_text)

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Normalization Complete",
                f"\tInput Text: {input_text}",
                f"\tOutput Text: {normalized_text}"]))

        return normalized_text

    def process(self):
        keys = self._df_regression['ExpectedResult'].unique()

        results = []

        for key in keys:
            df_key = self._df_regression[self._df_regression['ExpectedResult'] == key]

            def _vendor():
                vendors = df_key['Vendor'].unique()
                if vendors:
                    return vendors[0]

            vendor = _vendor()

            for input_text in df_key['Input'].unique():

                normalized_text = self._normalize(input_text)
                if not normalized_text:
                    continue

                has_match = key == normalized_text

                # the redundancy in variables is intentional;
                # this DataFrame is consistent with other DataFrames
                # produced by other regression suites
                results.append({
                    "Vendor": vendor,
                    "ExpectedResult": key,
                    "ActualResults": normalized_text,
                    "Pass": has_match,
                    "InputText": input_text,
                    "NormalizedText": normalized_text})

        return pd.DataFrame(results)
