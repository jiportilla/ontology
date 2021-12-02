#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import os

import pandas as pd
from pandas import DataFrame
from tabulate import tabulate

from base import BaseObject


class LoadPythonCollection(BaseObject):
    """ Load a Python Collection as DataFrame """

    def __init__(self,
                 file_path: str,
                 is_debug: bool = False):
        """
        Created:
            24-Dec-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/-cdo/unstructured-analytics/issues/1542#issuecomment-16802985
        :param file_path:
            a path to a single Python file
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)

        self._is_debug = is_debug
        self._file_path = file_path

    def process(self) -> DataFrame:
        input_path = os.path.join(os.environ["CODE_BASE"],
                                  "resources/output/transform",
                                  "parse_unstrut-int-import_20191224.csv")
        df = pd.read_csv(input_path, sep='\t', encoding='utf-8')

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Imported Internal Imports Collection",
                f"\tTotal Records: {len(df)}",
                f"\tInput Path: {input_path}",
                tabulate(df.sample(3), tablefmt='psql', headers='keys')]))

        return df
