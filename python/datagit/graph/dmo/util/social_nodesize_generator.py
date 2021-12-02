# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from typing import Optional

from pandas import DataFrame

from base import BaseObject


class SocialNodeSizeGenerator(BaseObject):
    """ Generate the Node Size (height/width) for a 'Person' node """

    __bucket_1 = 0.50
    __bucket_2 = 0.75
    __bucket_3 = 1.00
    __bucket_4 = 1.25
    __bucket_5 = 1.50

    def __init__(self,
                 df: Optional[DataFrame],
                 is_debug: bool = True):
        """
        Created:
            2-Jan-2020
            craig.trim@ibm.com
            *   based on runtime use of distribution analysis
                https://github.ibm.com/-cdo/unstructured-analytics/issues/1680#issuecomment-16901723
            *   use weight bucketing
                https://github.ibm.com/-cdo/unstructured-analytics/issues/1678#issuecomment-16904887
        :param is_debug:
            if True     increase log output at DEBUG level
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._df = df

    def _zscore(self,
                name: str) -> float:
        if self._df is None:
            return 0.0

        df2 = self._df[self._df['Name'] == name.lower()]
        if df2.empty:
            return 0.0

        return float(list(df2['zScore'].unique())[0])

    def _bucket(self,
                z_score: float) -> float:
        if z_score < 0.25:
            return self.__bucket_1
        if z_score < 0.0:
            return self.__bucket_2
        if z_score > 3.0:
            return self.__bucket_5
        if z_score > 1.5:
            return self.__bucket_4
        return self.__bucket_3

    def process(self,
                person_name: str):
        z_score = self._zscore(person_name)
        weight = self._bucket(z_score)

        svcresult = {
            "height": weight,
            "width": weight}

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Generated Person Node Size",
                f"\tzScore: {z_score}",
                f"\tBucket: {weight}"]))

        return svcresult
