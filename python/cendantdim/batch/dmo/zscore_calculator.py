#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import statistics

import pandas as pd
from pandas import DataFrame
from tabulate import tabulate

from base import BaseObject


class ZScoreCalculator(BaseObject):
    """ Compute Z-Scores on Dimensions for a Single Record """

    def __init__(self,
                 df_schema_weights: DataFrame,
                 is_debug: bool = False):
        """
        Created:
            7-Aug-2019
            craig.trim@ibm.com
            *   refactored out of 'process-single-record'
        :param df_schema_weights:
            Schema 	                Weight
            cloud 	                1.0
            system administrator 	9.0
            database 	            5.0
            data science 	        4.0
            hard skill 	            11.0
            other 	                7.0
            soft skill 	            9.0
            project management 	    7.0
            service management 	    8.0
        """
        BaseObject.__init__(self, __name__)

        self._is_debug = is_debug
        self.df_schema_weights = df_schema_weights

    def process(self) -> DataFrame or None:
        """
        Purpose:
            Take a Schema Weights DataFrame and generate zScores
        :return:
            Schema 	                Weight 	zScore 	zScoreNorm
            cloud 	                1.0 	-1.9 	0.0
            system administrator 	9.0 	0.7 	2.6
            database 	            5.0 	-0.6 	1.3
            data science 	        4.0 	-0.9 	1.0
            hard skill 	            11.0 	1.4 	3.3
            other 	                7.0 	0.1 	2.0
            soft skill 	            9.0 	0.7 	2.6
            project management 	    7.0 	0.1 	2.0
            service management 	    8.0 	0.4 	2.3
        """
        weights = list(self.df_schema_weights.Weight)

        def stdev():
            a_stdev = statistics.stdev(weights)
            if a_stdev == 0:
                return 0.005
            return a_stdev

        _stdev = stdev()
        _mean = statistics.mean(weights)

        zscores = []
        for weight in weights:
            zscore = (weight - _mean) / _stdev
            zscores.append(round(zscore, 2))

        results = []
        for i, row in self.df_schema_weights.iterrows():
            results.append({
                "Schema": row["Schema"],
                "Weight": row["Weight"],
                "zScore": zscores[i],
                "zScoreNorm": round(zscores[i] - min(zscores), 1)})

        df_zscore = pd.DataFrame(results)
        if self._is_debug:
            self.logger.debug(tabulate(df_zscore,
                                       headers='keys',
                                       tablefmt='psql'))

        if df_zscore.empty:
            self.logger.warning("zScore Calculation Failure")
            return None

        return df_zscore
