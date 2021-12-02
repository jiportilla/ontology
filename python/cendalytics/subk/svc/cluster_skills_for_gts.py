# !/usr/bin/env python
# -*- coding: UTF-8 -*-


import os
from typing import Optional

import pandas as pd
from pandas import DataFrame
from pandas import Series

from base import BaseObject

OUTPUT_FILE_NAME = "04_GTS_Skill_Parent_Taxonomy.csv"


class ClusterSkillsForGTS(BaseObject):
    """ Cluster Skills for GTS Employees
    """

    def __init__(self,
                 input_file: str,
                 is_debug: bool = True):
        """
        Created:
            15-Jan-2020
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1740
        :param is_debug:
            if True     increase log output at DEBUG level
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._df = self._load_file(input_file)

    def _load_file(self,
                   input_file: str) -> DataFrame:
        path = os.path.join(os.environ['GTS_BASE'],
                            input_file)
        if not os.path.exists(path):
            self.logger.error('\n'.join([
                "Collection File Not Found",
                f"\tFile Path: {path}"]))
            raise ValueError

        df = pd.read_csv(path, encoding="utf-8", sep="\t")
        if self._is_debug:
            self.logger.debug('\n'.join([
                "Loaded Input File",
                f"\tFile Path: {path}",
                f"\tTotal Records: {len(df)}"]))

        return df

    def process(self):
        from cendalytics.subk.dmo import SkillFileWriter
        from cendalytics.subk.dmo import SkillParentTaxonomy

        def generator(a_row: Series,
                      a_p1: Optional[str],
                      a_p2: Optional[str],
                      a_p3: Optional[str]) -> dict:
            return {
                'Tag': a_row['Tag'],
                'FieldID': a_row['FieldID'],
                'KeyField': a_row['KeyField'],
                'DivField': a_row['DivField'],
                'FieldName': a_row['FieldName'],
                'Confidence': a_row['Confidence'],
                'Parent1': a_p1,
                'Parent2': a_p2,
                'Parent3': a_p3}

        df_cluster = SkillParentTaxonomy(df_input=self._df,
                                         is_debug=self._is_debug).process(generator)

        SkillFileWriter(df_output=df_cluster,
                        output_file_name=OUTPUT_FILE_NAME).process()


def main():
    IS_DEBUG = True
    input_file = "resources/output/collections/supply_tag_20200116-.csv"
    ClusterSkillsForGTS(is_debug=IS_DEBUG,
                        input_file=input_file).process()


if __name__ == "__main__":
    main()
