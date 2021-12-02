#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import statistics
from collections import Counter

import pandas as pd
from pandas import DataFrame

from base import BaseObject
from base import FileIO
from datadict import FindDimensions


class WikipediaReferenceGenerator(BaseObject):
    """
    Prereqs:
        `source admin.sh wiki lookup`
        `source admin.sh wiki tag`
    """

    def __init__(self,
                 xdm_schema: str = 'supply'):
        """
        Created:
            8-May-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/143
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
        """
        BaseObject.__init__(self, __name__)
        self._input = self._input_file()
        self._tag_counter = Counter()  # overall tag counter
        self._dim_finder = FindDimensions(xdm_schema)

    @staticmethod
    def _input_file() -> dict:
        relative_path = "resources/output/wikipedia/dbpedia_02_summary_tagging.json"
        return FileIO.file_to_json(relative_path)

    def _to_dataframe(self) -> DataFrame:
        """ Create a DataFrame containing the frequency zScore for each tag frequency """

        def _results() -> list:
            """ create a list of key/value paired results """
            results = []
            for x in self._tag_counter:
                results.append({"Tag": x, "Frequency": self._tag_counter[x]})
            return results

        def _zscores(a_df: DataFrame):
            """ compute the zscores for each value """
            mean = statistics.mean(list(a_df.Frequency))
            stdev = statistics.stdev(list(a_df.Frequency))

            def _zscore(a_value: float) -> float:
                return (a_value - mean) / stdev

            return [_zscore(float(x))
                    for x in list(a_df.Frequency)]

        df = pd.DataFrame(_results())
        df["zScore"] = _zscores(df)

        return df

    def process(self) -> dict:
        """ build a dictionary of tag frequencies keyed to wikipedia pages """
        d_results = {}

        def _is_valid(a_tag: str,
                      a_page_schema: str) -> bool:
            for tag_schema in self._dim_finder.find(a_tag):
                if a_page_schema == tag_schema:
                    return a_tag.lower() != page["title"].lower()

        def _counter(a_page: dict) -> Counter:
            """ count the extracted tags on each wikipedia page """
            c = Counter()

            def _update(a_tag: str):
                c.update({a_tag: 1})
                self._tag_counter.update({a_tag: 1})

            for page_schema in self._dim_finder.find(a_page["title"]):
                for result in a_page["svcresults"]:
                    [_update(x)
                     for x in result["tags"]["supervised"]
                     if _is_valid(a_tag=x, a_page_schema=page_schema)]

            return c

        for page in self._input:
            _counter(page)

        df = self._to_dataframe()

        for page in self._input:
            d_tags = dict(_counter(page))

            _d = {}
            for tag in d_tags:
                zscore = round(df[df.Tag == tag]["zScore"].unique()[0], 2)
                _d[tag] = {"z": zscore, "f": d_tags[tag]}

            d_results[page["title"]] = _d

        return d_results
