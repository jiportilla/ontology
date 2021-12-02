#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pprint
from statistics import mean
from statistics import stdev

from base import BaseObject


class MeanGenerationByList(BaseObject):
    """
    Purpose:
        Generate JRS Means from a list of XDM records
    """

    def __init__(self,
                 records: list,
                 is_debug: bool = False):
        """
        Created:
            1-Oct-2019
            craig.trim@ibm.com
            *   refactored out of 'generate-means-records'
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1029
        """
        BaseObject.__init__(self, __name__)

        self._is_debug = is_debug
        self._records = records

    @staticmethod
    def _mean(values: list) -> float:
        return float(round(mean(values), 3))

    @staticmethod
    def _stdev(values: list) -> float:
        return float(round(stdev(values), 3))

    def process(self) -> dict:
        d_weights = {}
        for record in self._records:
            for key in record["slots"]:
                if key not in d_weights:
                    d_weights[key] = []
                d_weights[key].append(record["slots"][key]["weight"])

        d_mean = {key: {"mean": self._mean(d_weights[key]),
                        "stdev": self._stdev(d_weights[key])} for key in d_weights}

        if self._is_debug:
            self.logger.debug('\n'.join([
                f"Generated Means for Weights (total-records={len(self._records)})",
                pprint.pformat(d_mean)]))

        return d_mean
