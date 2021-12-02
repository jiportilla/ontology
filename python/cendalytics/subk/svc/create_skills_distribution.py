# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from collections import Counter
from statistics import mean
from statistics import stdev

import pandas as pd
from pandas import DataFrame
from tabulate import tabulate

from base import BaseObject

OUTPUT_FILE_NAME = "06_Skill_Distribution.csv"


class CreateSkillsDistribution(BaseObject):
    """ Create a Skills Distribution and Threshold

    Sample Output:
    """

    def __init__(self,
                 input_file: str,
                 is_debug: bool = True):
        """
        Created:
            20-Jan-2020
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1740#issuecomment-17265684
        :param is_debug:
            if True     increase log output at DEBUG level
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._input_file = input_file

    def _load_file(self) -> DataFrame:
        df = pd.read_csv(self._input_file, sep="\t", encoding="utf-8")
        self.logger.debug('\n'.join([
            "Loaded Input File",
            f"\tInput Path: {self._input_file}",
            f"\tTotal Records: {len(df)}",
            tabulate(df.sample(5),
                     headers='keys',
                     tablefmt='psql')]))

        return df

    @staticmethod
    def _count_skills(df: DataFrame) -> Counter:
        c = Counter()

        for _, row in df.iterrows():
            if type(row['Skills']) != str:
                continue

            skills = [x.strip() for x in row['Skills'].split(',')]
            [c.update({skill: 1}) for skill in skills]

        return c

    @staticmethod
    def _distribution(c: Counter) -> DataFrame:
        m = mean([c[x] for x in c])
        s = stdev([c[x] for x in c])

        results = []
        for x in c:
            z = (c[x] - m) / s
            results.append({
                "Skill": x,
                "Mean": m,
                "Stdev": s,
                "zScore": z})

        return pd.DataFrame(results)

    def _merge(self,
               df_input: DataFrame,
               df_distribution: DataFrame) -> DataFrame:
        results = []
        for _, row in df_input.iterrows():
            if type(row['Skills']) != str:
                continue

            skills = [x.strip() for x in row['Skills'].split(',')]

            for skill in skills:
                df2 = df_distribution[df_distribution['Skill'] == skill]

                def field_value(a_field_name: str):
                    return sorted(df2[a_field_name].unique())[0]

                results.append({
                    "CNUM": row['CNUM'],
                    "Source": row['Source'],
                    "Skill": skill,
                    "Mean": field_value("Mean"),
                    "Stdev": field_value("Stdev"),
                    "zScore": field_value("zScore")})

        return pd.DataFrame(results)

    def process(self):
        from cendalytics.subk.dmo import SkillFileWriter

        df_input = self._load_file()
        df_distribution = self._distribution(self._count_skills(df_input))
        df_output = self._merge(df_input, df_distribution)

        SkillFileWriter(df_output=df_output,
                        output_file_name=OUTPUT_FILE_NAME).process()


def main():
    IS_DEBUG = True
    input_file = "resources/output/subk/05_Skill_Cluster_Comparison.csv"
    CreateSkillsDistribution(is_debug=IS_DEBUG,
                             input_file=input_file).process()


if __name__ == "__main__":
    main()
