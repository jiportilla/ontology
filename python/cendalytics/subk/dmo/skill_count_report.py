# !/usr/bin/env python
# -*- coding: UTF-8 -*-


import os
from collections import Counter

import pandas as pd
from pandas import DataFrame

from base import BaseObject

OUTPUT_FILE_NAME = "01_Subk_Raw_Skill_Count.csv"


class SkillCountReport(BaseObject):
    """ Generate a Simple Skills Count Report """

    __d_transformations = {

        'ember': 'emberjs',

        'pearl': 'perl',
        'pel': 'perl',

        'ror': 'ruby on rails',
        'rsa': 'rational software architect',

        'was': 'websphere application server'
    }

    def __init__(self,
                 df_input: DataFrame,
                 is_debug: bool = True):
        """
        Created:
            15-Jan-2020
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1740#issuecomment-17171188
        Updated:
            17-Jan-2020
            craig.trim@ibm.com
            *   modify report generation
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1740#issuecomment-17225065
        :param is_debug:
            if True     increase log output at DEBUG level
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._df_input = df_input

        # key CNUMs to tokenized Skills list
        self._d_cnum_to_skills = {}

    def _transform(self,
                   input_text: str) -> str:
        for k in self.__d_transformations:
            if k in input_text:
                input_text = input_text.replace(k, self.__d_transformations[k])

        return input_text

    @staticmethod
    def _preprocess(input_text: str) -> str:
        input_text = input_text.replace('\n', ' ')
        input_text = input_text.replace('"', '')
        input_text = input_text.replace('\t', ' ')
        input_text = input_text.replace(':', ',')
        input_text = input_text.replace('/', ',')
        input_text = input_text.replace(' and ', ',')
        input_text = input_text.replace(' & ', ',')
        input_text = input_text.replace('(', ',')
        input_text = input_text.replace(')', ',')
        input_text = input_text.replace(' - ', ',')
        input_text = input_text.replace('-', ',')

        while '  ' in input_text:
            input_text = input_text.replace('  ', ' ')

        return input_text

    def _count_skills(self) -> Counter:
        c = Counter()

        blacklist = ['na']

        def _is_valid(a_value: str) -> bool:
            if not a_value:
                return False
            if type(a_value) != str:
                return False
            if a_value in blacklist:
                return False
            return True

        for _, row in self._df_input.iterrows():

            skill_list = row['Skills']
            if not _is_valid(skill_list):
                continue

            skill_list = self._preprocess(skill_list)
            if not skill_list or not len(skill_list):
                continue

            skill_list = self._transform(skill_list)
            if not skill_list or not len(skill_list):
                continue

            skills = [x.lower().strip() for x in skill_list.split(',')]
            skills = [x for x in skills if x and len(x)]

            [c.update({skill.lower(): 1}) for skill in skills]
            self._d_cnum_to_skills[row["CNUM"]] = skills

        return c

    @staticmethod
    def _output_file():
        full_path = os.path.join(os.environ['GTS_BASE'],
                                 'resources/output/subk')

        if not os.path.exists(full_path):
            os.makedirs(full_path, exist_ok=True)

        return os.path.join(full_path,
                            OUTPUT_FILE_NAME)

    def _to_dataframe(self,
                      c: Counter) -> DataFrame:
        results = []

        for _, row in self._df_input.iterrows():
            if row["CNUM"] not in self._d_cnum_to_skills:
                self.logger.warning(f"CNUM Not Found: {row['CNUM']}")
                continue

            skills = self._d_cnum_to_skills[row["CNUM"]]

            for skill in skills:
                results.append({
                    "CNUM": row["CNUM"],
                    "NotesID": row["NotesID"],
                    "Country": row["Country"],
                    "Skill": skill,
                    "Freq": c[skill]})

        return pd.DataFrame(results)

    def _write_to_file(self,
                       df_output: DataFrame) -> None:

        output_file_path = self._output_file()
        df_output.to_csv(output_file_path,
                         encoding="utf-8",
                         sep="\t")

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Wrote To File",
                f"\tOutput Path: {output_file_path}",
                f"\tTotal Records: {len(df_output)}"]))

    def process(self) -> DataFrame:
        count_of_skills = self._count_skills()
        df_output = self._to_dataframe(count_of_skills)
        self._write_to_file(df_output)

        return df_output
