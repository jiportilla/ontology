# !/usr/bin/env python
# -*- coding: UTF-8 -*-


import os

import pandas as pd
from pandas import DataFrame
from tabulate import tabulate

from base import BaseObject
from nlusvc import TextAPI

OUTPUT_FILE_NAME = "02_Subk_Skill_Tag_Analysis.csv"


class SkillParseReport(BaseObject):
    """ Generate a Report that shows Parse (Tag) output for Skills """

    __api = TextAPI(is_debug=False,
                    ontology_name='base')

    def __init__(self,
                 df_input: DataFrame,
                 is_debug: bool = True):
        """
        Created:
            15-Jan-2020
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1740#issuecomment-17171385
        Updated:
            17-Jan-2020
            craig.trim@ibm.com
            *   minor modifications in support of
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1740#issuecomment-17225065
        :param is_debug:
            if True     increase log output at DEBUG level
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._df_input = df_input

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Initialize SkillParseReport",
                f"\tSkill Count (size={len(self._df_input)})",
                tabulate(self._df_input.sample(3),
                         tablefmt='psql',
                         headers='keys')]))

    def _parse(self) -> DataFrame:

        def _tags(a_skill: str) -> list:

            df_result = self.__api.parse(a_skill)
            if df_result.empty:
                return []

            return sorted(df_result['Tag'].unique())

        results = []
        for _, row in self._df_input.iterrows():
            cnum = row['CNUM']
            skill = row['Skill']
            country = row['Country']
            notes_id = row['NotesID']
            freq = row['Freq']
            tag = row['Tag']

            # tags = _tags(skill)

            if not tag:
                results.append({
                    "CNUM": cnum,
                    "Skill": skill,
                    "Tag": "Unknown",
                    "Country": country,
                    "NotesID": notes_id,
                    "Freq": freq})
            else:
                results.append({
                    "Tag": tag,
                    "CNUM": cnum,
                    "Skill": skill,
                    "Country": country,
                    "NotesID": notes_id,
                    "Freq": freq})

        return pd.DataFrame(results)

    @staticmethod
    def _output_file():
        full_path = os.path.join(os.environ['GTS_BASE'],
                                 'resources/output/subk')

        if not os.path.exists(full_path):
            os.makedirs(full_path, exist_ok=True)

        return os.path.join(full_path,
                            OUTPUT_FILE_NAME)

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
        df_output = self._parse()

        self._write_to_file(df_output)

        return df_output
