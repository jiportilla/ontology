#!/usr/bin/env python
# -*- coding: UTF-8 -*-




import pandas as pd
from pandas import DataFrame

from base import BaseObject


class DBPediaRedirectReader(BaseObject):
    """ read extracted dbPedia redirects """

    def __init__(self,
                 input_file: str,
                 is_debug: bool = False):
        """
        Created:
            10-Jan-2020
            craig.trim@ibm.com
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._input_file = input_file

    def process(self) -> DataFrame:
        return pd.read_csv(self._input_file, encoding="utf-8", sep="\t")
