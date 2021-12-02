# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from typing import Optional

from pandas import DataFrame

from base import BaseObject


class SocialRelWeightGenerator(BaseObject):
    """ Use Batch-level Analysis to influence Graphviz Edge Style """

    def __init__(self,
                 df: Optional[DataFrame],
                 is_debug: bool = True):
        """
        Created:
            31-Dec-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1680
        Updated:
            2-Jan-2020
            craig.trim@ibm.com
            *   don't rely on a file for social collocation; use a passed-in dataframe
            *   renamed from 'graph-edgestyle-analysis' for consistency
                https://github.ibm.com/-cdo/unstructured-analytics/issues/1680#issuecomment-16901723
        :param is_debug:
            if True     increase log output at DEBUG level
        """
        BaseObject.__init__(self, __name__)
        self._df = df
        self._is_debug = is_debug

    def social_collocation(self,
                           person_a: str,
                           person_b: str) -> float:
        """
        Purpose:
            Return the Collocation Score between two individuals
        Warning:
            !!CASE SENSITIVE INPUT!!
        :param person_a:
            the name of an individual within this GitHub Repo
            Sample Input:
                'abhbasu'
        :param person_b:
            the name of another individual within this GitHub Repo
            Sample Input:
                'Craig-Trim'
        :return:
            the Z-Score indicating collocation strength
            Sample Output:
                6.203959000721737
        """
        if person_a == person_b:
            self.logger.error('\n'.join([
                "Identical Params",
                f"\tPerson A: {person_a}",
                f"\tPerson B: {person_b}"]))
            return 0.0

        names = [person_a.lower().strip(), person_b.lower().strip()]
        key = ','.join(sorted(names))
        df2 = self._df[self._df['Name'] == key]
        if df2.empty:
            self.logger.error('\n'.join([
                "No Collocation Score Found",
                f"\tPerson A: {person_a}",
                f"\tPerson B: {person_b}",
                f"\tKey: {key}"]))
            return 0.0

        return float(sorted(df2['zScore'].unique())[0])
