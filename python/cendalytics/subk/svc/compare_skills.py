# !/usr/bin/env python
# -*- coding: UTF-8 -*-


import os
import time

import pandas as pd
from pandas import DataFrame

from base import BaseObject

OUTPUT_FILE_NAME = "05_Skill_Cluster_Comparison.csv"


class CompareSkills(BaseObject):
    """ Compare Clustered Skills between GTS and Subk """

    def __init__(self,
                 file_name_sub: str,
                 file_name_: str,
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

        self._df_skills_sub = self._load_dataframe(file_name_sub)
        self._df_skills_ = self._load_dataframe(file_name_)

    def _load_dataframe(self,
                        file_name: str) -> DataFrame:
        absolute_path = os.path.join(os.environ['GTS_BASE'],
                                     "resources/output/subk",
                                     file_name)

        start = time.time()
        df = pd.read_csv(absolute_path, encoding="utf-8", sep="\t")

        if self._is_debug:
            total_time = round(time.time() - start, 2)
            self.logger.debug('\n'.join([
                "Loaded DataFrame",
                f"\tInput Path: {absolute_path}",
                f"\tTotal Records: {len(df)}",
                f"\tTotal Time: {total_time}s"]))

        return df

    def process(self):
        from cendalytics.subk.dmo import SkillFileWriter
        from cendalytics.subk.dmo import SkillClusterComparison

        df_output = SkillClusterComparison(is_debug=self._is_debug,
                                           df_skills_=self._df_skills_,
                                           df_skills_sub=self._df_skills_sub).process()

        SkillFileWriter(df_output=df_output,
                        output_file_name=OUTPUT_FILE_NAME).process()


def main():
    IS_DEBUG = True

    CompareSkills(is_debug=IS_DEBUG,
                  file_name_sub="03_Subk_Skill_Parent_Taxonomy.csv",
                  file_name_="04_GTS_Skill_Parent_Taxonomy.csv").process()


if __name__ == "__main__":
    main()
