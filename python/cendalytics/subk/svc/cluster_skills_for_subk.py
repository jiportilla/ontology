# !/usr/bin/env python
# -*- coding: UTF-8 -*-


import os
from typing import Optional

import pandas as pd
from pandas import DataFrame
from pandas import Series

from base import BaseObject
from datadict import FindRelationships

OUTPUT_FILE_NAME = "03_Subk_Skill_Parent_Taxonomy.csv"


class ClusterInputSkills(BaseObject):
    """ Cluster Input Skills by Parents (1, 2, 3) levels deep

    Sample Output:
        +-----+--------+-----------------------------+----------------------+----------------------+--------------------------+--------------------+
        |     |   Freq | Parent1                     | Parent2              | Parent3              | Skill                    | Tag                |
        |-----+--------+-----------------------------+----------------------+----------------------+--------------------------+--------------------|
        | 149 |      2 | shell script                | scripting language   | programming language | bash                     | Bash               |
        | 186 |      1 | web language                | programming language | formal language      | ember.js                 | Javascript         |
        | 241 |      1 | project management software | software             | product              | jira                     | JIRA               |
        | 344 |      1 | python library              | software library     | software             | scikit                   | Sklearn            |
        | 224 |      1 | mainframe operating system  | mainframe            | system               | aix                      | IBM AIX            |
        +-----+--------+-----------------------------+----------------------+----------------------+--------------------------+--------------------+
    """

    def __init__(self,
                 input_file: str,
                 is_debug: bool = True):
        """
        Created:
            16-Jan-2020
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1740#issuecomment-17198866
        :param is_debug:
            if True     increase log output at DEBUG level
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._input_file = input_file
        self._rel_finder = FindRelationships(is_debug=is_debug)

    def _load_file(self) -> DataFrame:
        def input_path() -> str:
            return os.path.join(os.environ['GTS_BASE'],
                                self._input_file)

        df = pd.read_csv(input_path(),
                         encoding="utf-8",
                         sep="\t")

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Loaded Input (Skill Parse Report)",
                f"\tInput File: {input_path()}",
                f"\tTotal Size: {len(df)}"]))

        return df

    def process(self):
        from cendalytics.subk.dmo import SkillFileWriter
        from cendalytics.subk.dmo import SkillParentTaxonomy

        def generator(a_row: Series,
                      a_p1: Optional[str],
                      a_p2: Optional[str],
                      a_p3: Optional[str]) -> dict:
            return {
                'Freq': a_row['Freq'],
                'Skill': a_row['Skill'],
                'CNUM': a_row['CNUM'],
                'NotesID': a_row['NotesID'],
                'Country': a_row['Country'],
                'Tag': a_row['Tag'],
                'Parent1': a_p1,
                'Parent2': a_p2,
                'Parent3': a_p3}

        df_cluster = SkillParentTaxonomy(is_debug=self._is_debug,
                                         df_input=self._load_file()).process(generator)

        SkillFileWriter(df_output=df_cluster,
                        output_file_name=OUTPUT_FILE_NAME).process()


def main():
    IS_DEBUG = True
    input_file = "resources/output/subk/02_Subk_Skill_Tag_Analysis.csv"
    ClusterInputSkills(is_debug=IS_DEBUG,
                       input_file=input_file).process()


if __name__ == "__main__":
    main()
