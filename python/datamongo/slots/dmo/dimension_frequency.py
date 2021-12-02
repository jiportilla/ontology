#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pandas as pd
from pandas import DataFrame

import statistics
from collections import Counter

from base import BaseObject
from base import MandatoryParamError


class DimensionFrequency(BaseObject):
    """ Create a DataFrame (over a Counter) of Dimension Frequency """

    def __init__(self,
                 some_records: list):
        """
        Created:
            30-Apr-2019
            craig.trim@ibm.com
        """
        BaseObject.__init__(self, __name__)
        if not some_records:
            raise MandatoryParamError("Records")

        self.records = some_records

    def process(self) -> DataFrame:

        c = Counter()
        for record in self.records:

            for slot in record["slots"]:
                c.update({slot: record["slots"][slot]["weight"]})

        weights = [c[x] for x in c]
        mean = statistics.mean(weights)
        stdev = statistics.stdev(weights)

        def _zscore(x: float) -> float:
            def _stdev():
                if stdev <= 0:
                    return 0.001
                return stdev

            return x - mean / _stdev()

        zscores = [_zscore(c[x]) for x in c]

        results = []
        for record in self.records:

            for slot in record["slots"]:
                weight = record["slots"][slot]["weight"]
                results.append({
                    'Frequency': weight,
                    "zScore": weight - mean / stdev})

        # c, d_slot = self._counter()
        # if as_dataframe:
        #     return self.to_dataframe(c, d_slot)
        # return c
        return pd.DataFrame(results)
