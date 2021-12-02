# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from collections import Counter

import pandas as pd
from pandas import DataFrame
from tabulate import tabulate

from base import BaseObject
from base import MandatoryParamError
from datamongo import BaseMongoClient
from datamongo import CendantCollection


class GenerateDivisionDistribution(BaseObject):
    """
    Purpose:
        Generate a record counter by distribution

    Sample Output:
        +----+---------+------------+
        |    |   Count | Division   |
        |----+---------+------------|
        |  1 |   96773 | gbs        |
        |  0 |   85485 |         |
        |  3 |   26969 | fno        |
        |  2 |   15203 | cloud      |
        |  4 |   12311 | systems    |
        |  5 |   11177 | chq_oth    |
        +----+---------+------------+
    """

    def __init__(self,
                 collection_name: str,
                 host_name: str = None,             # deprecated/ignored
                 is_debug: bool = True):
        """
        Created:
            28-Oct-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1209
        :param collection_name:
            any valid collection name that contains a 'div_field' at the record root
        :param host_name:
            deprecated/ignored
        :param is_debug:
            if True     increase log output at DEBUG level
        """
        BaseObject.__init__(self, __name__)
        if not collection_name:
            raise MandatoryParamError("Collection Name")

        self._is_debug = is_debug
        self._collection = CendantCollection(is_debug=self._is_debug,
                                             some_collection_name=collection_name,
                                             some_base_client=BaseMongoClient())

    @staticmethod
    def _count(records: list) -> Counter:
        c = Counter()
        for record in records:
            c.update({record["div_field"]: 1})
        return c

    @staticmethod
    def _to_dataframe(c: Counter) -> DataFrame:
        results = []

        for k in c:
            results.append({"Division": k, "Count": float(c[k])})

        return pd.DataFrame(results).sort_values(by=['Count'], ascending=False)

    def process(self) -> DataFrame:
        df = self._to_dataframe(
            self._count(
                self._collection.all()))

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Division Distributed Generated",
                tabulate(df, headers='keys', tablefmt='psql')]))

        return df
