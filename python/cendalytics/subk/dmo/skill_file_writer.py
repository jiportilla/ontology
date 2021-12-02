# !/usr/bin/env python
# -*- coding: UTF-8 -*-


import os

from pandas import DataFrame

from base import BaseObject


class SkillFileWriter(BaseObject):
    """ Common Domain Component to Write DataFrames to File """

    def __init__(self,
                 df_output: DataFrame,
                 output_file_name: str,
                 is_debug: bool = True):
        """
        Created:
            20-Jan-2020
            craig.trim@ibm.com
        :param is_debug:
            if True     increase log output at DEBUG level
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._df_output = df_output
        self._output_file_name = output_file_name

    def process(self) -> None:

        full_path = os.path.join(os.environ['GTS_BASE'],
                                 'resources/output/subk')

        if not os.path.exists(full_path):
            os.makedirs(full_path, exist_ok=True)

        output_path = os.path.join(full_path,
                                   self._output_file_name)

        self._df_output.to_csv(output_path,
                               encoding="utf-8",
                               sep="\t")

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Wrote To File",
                f"\tOutput Path: {output_path}",
                f"\tTotal Records: {len(self._df_output)}"]))
