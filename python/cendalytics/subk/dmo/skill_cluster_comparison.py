# !/usr/bin/env python
# -*- coding: UTF-8 -*-


import pandas as pd
from pandas import DataFrame
from tabulate import tabulate

from base import BaseObject


class SkillClusterComparison(BaseObject):
    """ Compare Clustered Skills between GTS and Subk """

    __blacklist = [
        'activity',
        'artifact',
        'age',

        'benefit',
        'behavior',

        'company',

        'document',

        'entity',

        'individual role',

        'professional role',
        'property',

        'root',
    ]  # terms we are not interested in

    def __init__(self,
                 df_skills_sub: DataFrame,
                 df_skills_: DataFrame,
                 is_debug: bool = True):
        """
        Created:
            17-Jan-2020
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1740#issuecomment-17225986
        :param is_debug:
            if True     increase log output at DEBUG level
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._df_skills_sub = df_skills_sub
        self._df_skills_ = df_skills_

    def _cluster(self,
                 source: str,
                 df: DataFrame,
                 key_field_name: str,
                 aggregation_level: str = 'Parent1') -> []:

        results = []
        key_fields = sorted(df[key_field_name].unique())

        ctr = 0
        total = len(key_fields)

        for key_field in key_fields:
            df2 = df[df[key_field_name] == key_field]

            cluster = sorted(df2[aggregation_level].unique())
            cluster = [x for x in cluster if x not in self.__blacklist]

            ctr += 1
            if ctr % 100 == 0:
                    self.logger.debug(f"Progress {ctr}-{total}")

            k = ', '.join(cluster)
            results.append({
                "Source": source,
                "Skills": k,
                "CNUM": key_field})

        return results

    def process(self) -> DataFrame:
        results = []

        results += self._cluster(source="subk",
                                 key_field_name='CNUM',
                                 df=self._df_skills_sub)

        results += self._cluster(source="",
                                 key_field_name='KeyField',
                                 df=self._df_skills_)

        df = pd.DataFrame(results)

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Generated Skill Cluster",
                f"\tTotal Records: {len(df)}",
                tabulate(df.sample(5),
                         tablefmt='psql',
                         headers='keys')]))

        return df
