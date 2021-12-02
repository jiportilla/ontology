# !/usr/bin/env python
# -*- coding: UTF-8 -*-


import os
from statistics import mean
from statistics import stdev
from typing import Optional

import pandas as pd
from pandas import DataFrame

from base import BaseObject


class GenerateSocialDistribution(BaseObject):
    """ Generate a Distribution of the Collocation Analysis for Social Relationships """

    def __init__(self,
                 df_rel_input: DataFrame,
                 df_ent_input: DataFrame,
                 is_debug: bool = True):
        """
        Created:
            31-Dec-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/-cdo/unstructured-analytics/issues/1680
        Updated:
            2-Jan-2020
            craig.trim@ibm.com
            *   use an entity file along with the relationships file
                https://github.ibm.com/-cdo/unstructured-analytics/issues/1680#issuecomment-16901010
        :param is_debug:
            if True     increase log output at DEBUG level
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._df_rel_input = df_rel_input
        self._df_ent_input = df_ent_input

    @staticmethod
    def _mean_and_stdev(df_input: DataFrame) -> dict:
        counts = []
        for _, row in df_input.iterrows():
            counts.append(row['Count'])

        m = mean(counts)
        s = stdev(counts)

        return {"mean": m, "stdev": s}

    @staticmethod
    def _generate_zscores(df_input: DataFrame,
                          d_params: dict) -> dict:
        d = {}
        for _, row in df_input.iterrows():
            z = (row['Count'] - d_params['mean']) / d_params['stdev']
            if z not in d:
                d[z] = set()
            d[z].add(row['Name'])
        return d

    @staticmethod
    def _to_dataframe(d_results: dict) -> DataFrame:
        results = []

        for z_score in d_results:
            for names in d_results[z_score]:
                results.append({
                    'Name': names,
                    'zScore': z_score})

        return pd.DataFrame(results)

    def _write_to_file(self,
                       df_result: DataFrame,
                       relative_output_path: str):
        output_path = os.path.join(os.environ['CODE_BASE'], relative_output_path)
        df_result.to_csv(output_path, encoding='utf-8', sep='\t')
        if self._is_debug:
            self.logger.debug('\n'.join([
                "Wrote Result to File",
                f"\tPath: {output_path}",
                f"\tTotal Records: {len(df_result)}"]))

    def _generate(self,
                  a_df: DataFrame,
                  relative_output_path: str,
                  write_to_file: bool) -> DataFrame:
        d_params = self._mean_and_stdev(a_df)
        d_results = self._generate_zscores(df_input=a_df,
                                           d_params=d_params)
        df = self._to_dataframe(d_results)

        if write_to_file and relative_output_path:
            self._write_to_file(df_result=df,
                                relative_output_path=relative_output_path)

        return df

    def process(self,
                write_to_file: bool = False,
                output_rel_relative_path: Optional[
                    str] = 'resources/output/analysis/graphviz_socialrel_distribution.csv',
                output_ent_relative_path: Optional[
                    str] = 'resources/output/analysis/graphviz_socialent_distribution.csv'):

        df_ent = self._generate(a_df=self._df_ent_input,
                                write_to_file=write_to_file,
                                relative_output_path=output_ent_relative_path)

        df_rel = self._generate(a_df=self._df_rel_input,
                                write_to_file=write_to_file,
                                relative_output_path=output_rel_relative_path)

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Distrubution Analysis Completed",
                f"\tTotal Entities: {len(df_ent)}",
                f"\tTotal Relationships: {len(df_rel)}"]))

        return {
            "ent": df_ent,
            "rel": df_rel}
