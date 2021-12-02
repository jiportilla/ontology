#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pandas as pd
from pandas import DataFrame
from tabulate import tabulate

from base import BaseObject
from base import MandatoryParamError
from datadict import FindDimensions
from datamongo import BaseMongoClient


class GenerateDataFrameStarPlot(BaseObject):
    """
    Purpose:

    """

    def __init__(self,
                 xdm_schema: str,
                 df_record: DataFrame,
                 mongo_client: BaseMongoClient = None,
                 is_debug: bool = False):
        """
        Created:
            13-May-2019
            craig.trim@ibm.com
            *   refactored out of dimensions-api
        Updated:
            29-Oct-2019
            craig.trim@ibm.com
            *   remove 'entity-schema-finder' in favor of new approach
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/916#issuecomment-15620801
        :param xdm_schema:
            the name of the schema to perform the type lookup
            Notes:
            -   typically either 'supply' or 'learning'
            -   the full list is on path 'resources/config/dimensionality'
        :param df_record:
        :param mongo_client:
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)
        if not xdm_schema:
            raise MandatoryParamError("XDM Schema")
        if len(df_record) == 0:
            raise MandatoryParamError("DataFrame")
        if not mongo_client:
            mongo_client = BaseMongoClient()

        self._is_debug = is_debug
        self._df_record = df_record
        self._mongo_client = mongo_client
        self._dim_finder = FindDimensions(xdm_schema)

        if self._is_debug:
            self.logger.debug("\n".join([
                "Instantiated Starplot Function",
                "\tSource Name: {}".format(xdm_schema),
                "\n{}".format(tabulate(df_record,
                                       headers='keys',
                                       tablefmt='psql'))]))

    def process(self,
                divisor: int = 10,
                minimum: int = 3) -> DataFrame:

        d_record = {'group': ['A']}

        def _update(a_key: str):

            def _value() -> float:
                values = self._df_record[self._df_record.Schema == a_key].Weight.unique()
                if len(values) == 0:
                    return 0.01

                x = float(values[0])
                if x > 0:
                    return float(x / divisor)
                return float(minimum)

            d_record[a_key] = [_value()]

        for entity in self._dim_finder.top_level_entities():
            _update(entity)

        return pd.DataFrame(d_record)
