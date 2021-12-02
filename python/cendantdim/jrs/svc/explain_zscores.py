#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from statistics import mean
from statistics import stdev

import pandas as pd
from pandas import DataFrame

from base import BaseObject


class ExplainZscores(BaseObject):
    """
    Purpose:
    Compute a zScore of zScores
    """

    def __init__(self,
                 df_zscores: DataFrame,
                 is_debug: bool = False):
        """
        Created:
            1-Oct-2019
            craig.trim@ibm.com
            *   transposed DataFrame
                https://github.ibm.com/-cdo/unstructured-analytics/issues/1014
        :param df_zscores:
            +----+----------------+-------------+----------------+---------------------+------------+-------------+----------+----------+
            |    |   HarmonicMean |   JobRoleId |   SerialNumber | Slot                |   SlotMean |   SlotStdev |   Weight |   zScore |
            |----+----------------+-------------+----------------+---------------------+------------+-------------+----------+----------|
            |  0 |           0.11 |      042393 |      050484766 | Blockchain          |      0.226 |       0.977 |    0     |        0 |
            |  1 |           0.02 |      042393 |      050484766 | Quantum             |      0.032 |       0.107 |    0     |        0 |
            |  2 |           1.34 |      042393 |      050484766 | Cloud               |      2.683 |       4.46  |    0     |       -1 |
            |  3 |           6.23 |      042393 |      050484766 | SystemAdministrator |      8.431 |      12.323 |    4.022 |        0 |
            |  4 |           1.7  |      042393 |      050484766 | Database            |      3.404 |       6.858 |    0     |        0 |
            |  5 |           3.66 |      042393 |      050484766 | DataScience         |      5.492 |      10.156 |    1.835 |        0 |
            |  6 |          14.43 |      042393 |      050484766 | HardSkill           |     19.523 |      16.744 |    9.34  |       -1 |
            |  7 |           4.55 |      042393 |      050484766 | Other               |      8.86  |      10.075 |    0.24  |       -1 |
            |  8 |          16.74 |      042393 |      050484766 | SoftSkill           |     18.07  |      16.234 |   15.401 |        0 |
            |  9 |           7.11 |      042393 |      050484766 | ProjectManagement   |     11.175 |      12.84  |    3.044 |       -1 |
            | 10 |           8.77 |      042393 |      050484766 | ServiceManagement   |     11.658 |      13.014 |    5.887 |        0 |
            +----+----------------+-------------+----------------+---------------------+------------+-------------+----------+----------+        :return:
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)
        self.is_debug = is_debug
        self._df_zscore = df_zscores

    @staticmethod
    def _zscore(single_zscore: float,
                zscore_mean: float,
                zscore_stdev: float) -> float:
        """
        Purpose:
            Compute a z-Score
        :param single_zscore:
            a single aggregated zScore for an CNUM
        :param zscore_mean:
            the Mean zScore across a population
        :param zscore_stdev:
            the Std. Dev. of zScores across a population
        :return:
            a z-score
        """

        def _stdev():
            if zscore_stdev <= 0.0:
                return 0.001
            return zscore_stdev

        z = round((single_zscore - zscore_mean) / _stdev(), 0)
        if z == -0.0:  # yes, this happens
            return 0.0
        return z

    def process(self) -> DataFrame:

        scores = []
        for cnum in self._df_zscore['SerialNumber'].unique():
            df_cnum = self._df_zscore[self._df_zscore['SerialNumber'] == cnum]
            scores.append(sum(df_cnum['zScore']))

        z_mean = mean(scores)
        z_stdev = stdev(scores)

        ctr = 0
        results = []

        cnums = self._df_zscore['SerialNumber'].unique()
        total_cnums = len(cnums)

        for cnum in cnums:

            ctr += 1
            if ctr % 100 == 0 and self.is_debug:
                self.logger.debug(f"Progress: ({ctr} - {total_cnums})")

            df_cnum = self._df_zscore[self._df_zscore['SerialNumber'] == cnum]
            job_role_id = list(df_cnum['JobRoleId'].unique())[0]
            harmonic_mean = list(df_cnum['HarmonicMean'].unique())[0]

            score = sum(df_cnum['zScore'])
            zscore = self._zscore(single_zscore=score,
                                  zscore_mean=z_mean,
                                  zscore_stdev=z_stdev)

            results.append({
                "SerialNumber": cnum,
                "JobRoleId": job_role_id,
                "hMean": harmonic_mean,
                "hDelta": harmonic_mean,
                "zScore": zscore,
                "Score": score,
                "zMean": z_mean,
                "zStdev": z_stdev})

        df = pd.DataFrame(results)
        return df.sort_values(by=['zScore'],
                              ascending=False)
