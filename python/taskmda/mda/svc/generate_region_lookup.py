#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import os

import pandas as pd
from pandas import DataFrame

from base import BaseObject


class GenerateRegionLookup(BaseObject):

    def __init__(self,
                 is_debug: bool = False):
        """
        Created:
            20-May-2019
            craig.trim@ibm.com
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug

    @staticmethod
    def _read_lookup() -> DataFrame:
        def _path() -> str:
            return os.path.join(os.environ["CODE_BASE"],
                                "resources/input/countries/geo-lookup.xlsx")

        return pd.read_excel(_path())

    def process(self):
        from taskmda.mda.dmo import CityRegionGenerator
        from taskmda.mda.dmo import CountryRegionGenerator

        df = self._read_lookup()
        CityRegionGenerator(df).process()
        CountryRegionGenerator(df).process()
