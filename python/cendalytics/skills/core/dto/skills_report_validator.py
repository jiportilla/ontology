# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from pandas import DataFrame

from base import BaseObject
from base import DataTypeError


class SkillsReportValidator(BaseObject):
    """
    Purpose:
    Validate the Data Types for the SkillsReportAPI

    Traceability:
    https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/354

    Created:
        13-Jun-2019
        craig.trim@ibm.com
    Updated:
        8-Nov-2019
        craig.trim@ibm.com
        *   refactoring in pursuit of
            https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1293
    """

    @classmethod
    def expected(cls,
                 actual_param,
                 expected_type: str):
        raise DataTypeError("\n".join([
            "Token (actual={}, expected={})".format(
                type(actual_param),
                expected_type)]))

    @classmethod
    def normalize(cls,
                  token: str) -> None:
        if type(token) != str:
            cls.expected(token, "str")

    @classmethod
    def variants(cls,
                 token: str) -> None:
        if type(token) != str:
            cls.expected(token, "str")

    @classmethod
    def graphviz(cls,
                 df: DataFrame,
                 engine: str) -> None:
        if type(df) != DataFrame:
            cls.expected(df, "pandas.DataFrame")
        if type(engine) != str:
            cls.expected(engine, "str")

    @classmethod
    def hash_serial_number(cls,
                           df: DataFrame,
                           column_name: str) -> None:
        if type(df) != DataFrame:
            cls.expected(df, "pandas.DataFrame")
        if type(column_name) != str:
            cls.expected(column_name, "str")
