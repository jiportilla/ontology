#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pandas as pd
from pandas import DataFrame

from base import BaseObject
from base import DataTypeError


class PerformPosAnalysis(BaseObject):
    """
    Perform Part-of-Speech (POS) analysis
    """

    def __init__(self,

                 is_debug: bool = False):
        """
        Created:
            9-Jul-2019
            craig.trim@ibm.com
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)
        self.is_debug = is_debug

    @staticmethod
    def _variations(multiple_dataframes: list) -> dict:
        from nlusvc.core.dmo import GenerateNounVariations

        _d_master = {"NN2": set(), "NN3": set(), "NN4": set(), "ADJNN": set(), "VCN": set()}
        _gen = GenerateNounVariations()

        def _update(d_terms: dict):
            _d_master["NN2"] = _d_master["NN2"].union(d_terms["NN2"])
            _d_master["NN3"] = _d_master["NN3"].union(d_terms["NN3"])
            _d_master["NN4"] = _d_master["NN4"].union(d_terms["NN4"])
            _d_master["ADJNN"] = _d_master["ADJNN"].union(d_terms["ADJNN"])
            _d_master["VCN"] = _d_master["VCN"].union(d_terms["VCN"])
            return _d_master

        for x in multiple_dataframes:
            _d_master = _update(_gen.process(x))

        return {
            "NN2": sorted(_d_master["NN2"]),
            "NN3": sorted(_d_master["NN3"]),
            "NN4": sorted(_d_master["NN4"]),
            "ADJNN": sorted(_d_master["ADJNN"]),
            "VCN": sorted(_d_master["VCN"])}

    def _from_dataframe(self,
                        df_part_of_speech: DataFrame):
        if type(df_part_of_speech) != DataFrame:
            raise DataTypeError("\n".join([
                "Invalid Input DataType"]))
        return self._variations([df_part_of_speech])

    def _from_list(self,
                   multiple_dataframes: list):
        if type(multiple_dataframes) != list:
            raise DataTypeError("\n".join([
                "Invalid Input DataType"]))
        return self._variations(multiple_dataframes)

    @staticmethod
    def _to_dataframe(d: dict) -> DataFrame:
        results = []
        for k in d:
            for v in d[k]:
                results.append({"Key": k, "Value": v})

        return pd.DataFrame(results)

    def process(self,
                some_input: DataFrame or list) -> DataFrame:
        def inner():
            if type(some_input) == DataFrame:
                return self._from_dataframe(some_input)
            elif type(some_input) == list:
                return self._from_list(some_input)
            else:
                raise DataTypeError("\n".join([
                    "Invalid Input DataType"]))

        results = self._to_dataframe(inner())

        return results
