#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import os
from typing import Optional

import pandas as pd
from pandas import DataFrame
from tabulate import tabulate

from base import BaseObject


class ParsePythonFiles(BaseObject):
    """ Parse Python Code from within a directory (recursively) """

    def __init__(self,
                 files: list,
                 is_debug: bool = False):
        """
        Created:
            5-Dec-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1521
        Updated:
            20-Dec-2019
            craig.trim@ibm.com
            *   fix unintended file dropout defect
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1633#issuecomment-16764230
        Updated:
            24-Dec-2019
            craig.trim@ibm.com
            *   renamed from 'perform-python-parse'
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1637#issuecomment-16802191
            *   refactored in pursuit of
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1642#issuecomment-16802836
        :param files:
            a list of paths to python files in a workspace
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)

        self._files = files
        self._is_debug = is_debug
        self._base_path = os.environ["CODE_BASE"]

    def process(self) -> Optional[DataFrame]:
        from dataingest.grammar.svc import ParsePythonFile

        results = []
        for file in self._files:
            results += ParsePythonFile(is_debug=self._is_debug,
                                       file_path=file).process()

        df = pd.DataFrame(results)

        def _sample() -> DataFrame:
            if len(df) > 3:
                return df.sample(3)
            return df

        self.logger.info('\n'.join([
            f"Python Directory Parse Complete (size={len(df)})",
            tabulate(_sample(), headers='keys', tablefmt='psql')]))

        return df
