#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pandas as pd
from pandas import DataFrame

from base import BaseObject


class RegionalRollupMapper(BaseObject):
    """ Perform a rollup from Countries to Regions based on an explicit mapping file """

    __input_path = "resources/config/other/sentiment-regional-rollups.csv"

    def __init__(self,
                 is_debug: bool = False):
        """
        Created:
            26-Nov-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1449
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)

        self._df = self._load()
        self._is_debug = is_debug

    def _load(self) -> DataFrame:
        return pd.read_csv(self.__input_path,
                           encoding='utf-8',
                           sep=',',
                           skiprows=0,
                           names=['Country', 'Region'])

    def lookup(self,
               country: str) -> str:

        if type(country) != str:
            self.logger.warning(f"Unrecognized Country ("
                                f"value={country}, "
                                f"type={type(country)})")
            return "Other"

        df_region = self._df[self._df['Country'] == country.lower()]
        if df_region.empty:
            self.logger.error(f"Country Not Found (name={country})")
            raise NotImplementedError

        return df_region['Region'].unique()[0]
