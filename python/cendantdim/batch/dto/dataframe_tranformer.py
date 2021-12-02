#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import logging

import pandas as pd
from pandas import DataFrame
from tabulate import tabulate

from base import DataTypeError
from datamongo import CendantCollectionCategory


class DataFrameTransformer:
    """ Data Frame Transformer """

    logger = logging.getLogger(str(__name__))

    @classmethod
    def to_summary_df(cls,
                      d_results: dict,
                      is_debug: bool = True) -> DataFrame:
        """
        Purpose:
            Transform a dictionary of DataFrame Weights into a single Summarized Dataframe
        :param d_results:
            {   key-field-1: <weights-df>,
                key-field-2: <weights-df>,
                ...
                key-field-n: <weights-df> }
        :param is_debug:
        :return:
            a summarized dataframe
        """
        if type(d_results) != dict:  # GIT-1415-16099298
            raise DataTypeError

        summary = []
        for key_field in d_results:
            df_result = d_results[key_field]

            for _, row in df_result.iterrows():
                summary.append({
                    "Schema": row["Schema"],
                    "Weight": row["Weight"],
                    "KeyField": key_field})

        df_summary = pd.DataFrame(summary)

        if is_debug:
            cls.logger.info("\n".join([
                "Total Records: {}".format(len(d_results)),
                "\n{}".format(tabulate(df_summary,
                                       headers='keys',
                                       tablefmt='psql'))]))

        return df_summary

    @classmethod
    def dict_to_df_schema_weights(cls,
                                  a_dict: dict) -> DataFrame:
        """
        Purpose:
            Transform a Dimension JSON Result from MongoDB into a Pandas DataFrame
        :param a_dict:
            Expected Format:
            {	slots: {
                    cloud: 0
                    system administrator: 3
                    database: 2
                    data science: 4
                    other: 1
                    soft skill: 2 }
                key_field: "URL-COURSE- V1:DELFTX+BMI.3X+3T2018"
                ts: "1557970557.1436677" }
        :return:
            a pandas DataFrame
        """
        if type(a_dict) != dict:  # GIT-1415-16099298
            raise DataTypeError

        results = []
        for k in a_dict["slots"]:
            results.append({"Schema": k,
                            "Weight": a_dict["slots"][k],
                            "KeyField": a_dict["key_field"]})

        return pd.DataFrame(results)

    @staticmethod
    def dimensionality_attributes(a_source_record: dict,
                                  collection_category: CendantCollectionCategory) -> dict:
        """
        Purpose:
            Store additional attributes with the dimensionality weights
            this will obviate the need for expensive lookups in later analysis
        :param a_source_record:
            a source record in this context is any record from one of the 'tag' collections
            e.g., supply-tag, demand-tag, learning-tag
        :param collection_category:
        :return:
            a dictionary of attributes
        """
        if type(a_source_record) != dict:  # GIT-1415-16099298
            raise DataTypeError

        def _by_field_name_as_int(a_field_name: str) -> int:
            for field in a_source_record["fields"]:
                if field["name"] == a_field_name:
                    result = field["value"][0]
                    if result:
                        try:
                            return int(result)
                        except ValueError:
                            pass
            return -1

        def _by_field_name(a_field_name: str) -> str:
            for field in a_source_record["fields"]:
                if field["name"] == a_field_name:
                    result = field["value"][0]
                    if result:
                        return result.lower()

        def _band_level():
            if collection_category == CendantCollectionCategory.DEMAND:
                return {"high_band": _by_field_name_as_int("high_band"),
                        "low_band": _by_field_name_as_int("low_band")}
            if collection_category == CendantCollectionCategory.SUPPLY:
                _band = _by_field_name_as_int("band")
                return {"high_band": _band,
                        "low_band": _band}
            return {}

        def _date_range():
            if collection_category == CendantCollectionCategory.DEMAND:
                return {"start_date": _by_field_name("start_date"),
                        "end_date": _by_field_name("end_date")}
            return {}

        def _division():
            return a_source_record["div_field"].lower()

        def _status():
            if collection_category == CendantCollectionCategory.DEMAND:
                return _by_field_name("status")
            return None

        def _job_role_id():
            for field in a_source_record["fields"]:
                if field["name"] == "job_role_id":
                    result = field["value"][0]
                    if result:
                        return result.lower()

        def _region():
            if collection_category == CendantCollectionCategory.DEMAND:
                return _by_field_name("region")
            if collection_category == CendantCollectionCategory.SUPPLY:
                return _by_field_name("region")
            return {}

        def _country():
            if collection_category == CendantCollectionCategory.DEMAND:
                return _by_field_name("country")
            if collection_category == CendantCollectionCategory.SUPPLY:
                return _by_field_name("country")
            return None

        return {"key_field": a_source_record["key_field"],
                "fields": {
                    "job_role_id": _job_role_id(),
                    "division": _division(),
                    "status": _status(),
                    "date": _date_range(),
                    "band": _band_level(),
                    "country": _country(),
                    "region": _region()}}

    @staticmethod
    def df_schema_weights_to_dict(a_df: DataFrame,
                                  **args) -> dict:
        """
        Transform a Dimension DataFrame to a MongoDB compatible Dictionary
        :param a_df:
            Expected Input:
                             Schema  Weight
            0                 cloud     0.0
            1  system administrator     1.0
            2              database     3.0
            3          data science     0.0
            4            hard skill     5.0
            5                 other     4.0
            6            soft skill     5.0
            7    project management     3.0
            8    service management     2.0
        :return:
            dict
        """
        if type(a_df) != DataFrame:  # GIT-1415-16099298
            raise DataTypeError

        d = {"slots": {}}
        for _, row in a_df.iterrows():
            d["slots"][row["Schema"]] = row["Weight"]

        for k in args:
            d[k] = args[k]

        return d

    @staticmethod
    def df_zscores_to_dict(a_df: DataFrame,
                           **args) -> dict:
        """
        Transform a zScores DataFrame to a MongoDB compatible Dictionary
        :param a_df:
            Expected Input:
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
        :return:
            dict
        """
        if type(a_df) != DataFrame:  # GIT-1415-16099298
            raise DataTypeError

        d_slots = {}

        for _, row in a_df.iterrows():
            d_slots[row["Schema"]] = {
                "weight": row["Weight"],
                "z_score": row["zScore"],
                "z_score_norm": row["zScoreNorm"]}

        d = {"slots": d_slots}
        for k in args:
            d[k] = args[k]

        return d

    @staticmethod
    def dict_to_df_zscores(a_dict: dict) -> DataFrame:
        """
        Purpose:
            Transform a Dimension JSON Result from MongoDB into a Pandas DataFrame
        Updated:
            15-Oct-2019
            craig.trim@ibm.com
            *   changed dictionary iteration logic
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1116#issuecomment-15309613
        :param a_dict:
            Expected Format:
                {   'key_field': '099711631',
                    'slots':
                        {   'cloud': {
                                'weight': 11.6,
                                'z_score': -0.77,
                                'z_score_norm': 0.2},
                            'data science': {
                                'weight': 8.8,
                                'z_score': -0.88,
                                'z_score_norm': 0.1},
                            ...
                            'database': {
                                'weight': 6.0,
                                'z_score': -1.0,
                                'z_score_norm': 0.0},
                        },
                    'ts': '1558032828.517984' }
        :return:
            a pandas DataFrame
        """
        if type(a_dict) != dict:  # GIT-1415-16099298
            raise DataTypeError

        results = []
        for k in a_dict["slots"]:

            if type(a_dict["slots"][k]) == dict:
                results.append({"Schema": k,
                                "Weight": a_dict["slots"][k]["weight"],
                                "zScore": a_dict["slots"][k]["z_score"],
                                "zScoreNorm": a_dict["slots"][k]["z_score_norm"]})
            elif type(a_dict["slots"][k] == float):
                results.append({"Schema": k,
                                "Weight": a_dict["slots"][k]})
            else:
                raise NotImplementedError

        return pd.DataFrame(results)
