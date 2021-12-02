#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from statistics import mean
from statistics import stdev

import pandas as pd
from pandas import DataFrame

from base import BaseObject


class MeanGenerationByDataFrame(BaseObject):
    """
    Purpose:
        Generate JRS Means from a DataFrame

    Sample Input:
        +------+-------------+----------------+---------------------+----------+
        |      |   JobRoleId | SerialNumber   | Slot                |   Mean   |
        |------+-------------+----------------+---------------------+----------|
        |    0 |      042393 | 050484766      | Blockchain          |   0      |
        |    1 |      042393 | 050484766      | Quantum             |   0      |
        |    2 |      042393 | 050484766      | Cloud               |   0      |
        |    3 |      042393 | 050484766      | SystemAdministrator |   4.022  |
        |    4 |      042393 | 050484766      | Database            |   0      |
        ...
        | 1098 |      042393 | 9D5863897      | ProjectManagement   |   0      |
        | 1099 |      042393 | 9D5863897      | ServiceManagement   |   4.411  |
        +------+-------------+----------------+---------------------+----------+
    Input Generator:
        'generate-jrs-mapping.py'

    Sample Output:

    """

    def __init__(self,
                 df: DataFrame,
                 is_debug: bool = False):
        """
        Created:
            1-Oct-2019
            craig.trim@ibm.com
            *   refactored out of 'generate-means-records'
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1029
        """
        BaseObject.__init__(self, __name__)

        self._df = df
        self._is_debug = is_debug

    @staticmethod
    def _mean(values: list) -> float:
        return float(round(mean(values), 3))

    @staticmethod
    def _stdev(values: list) -> float:
        return float(round(stdev(values), 3))

    def process(self) -> DataFrame:
        results = []

        slots = sorted(self._df['Slot'].unique())

        for slot in slots:
            df2 = self._df[self._df['Slot'] == slot]
            values = list(df2['Weight'])

            results.append({"Slot": slot,
                            "Mean": self._mean(values),
                            "Stdev": self._stdev(values)})

        return pd.DataFrame(results)
