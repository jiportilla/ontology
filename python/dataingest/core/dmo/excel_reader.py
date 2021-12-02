#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import pandas as pd
from pandas.core.frame import DataFrame

from base import BaseObject


class ExcelReader(BaseObject):
    """ Read an Excel File into a pandas DataFrame
    """

    @staticmethod
    def read_excel(some_input_path: str,
                   skiprows: int,
                   column_names: dict,
                   some_sheet_name=None,
                   usecols=None) -> DataFrame:
        """ read a single worksheet out of an excel workbook
            into a pandas dataframe
        :param some_input_path : str:
            path to file
        :param skiprows : list-like:
            Rows to skip at the beginning (0-indexed)
        :param column_names:
            Data type for data or columns. E.g. {'a': np.float64, 'b': np.int32}
        :param some_sheet_name : string, int, mixed list of strings/ints, or None, default 0:
            Strings are used for sheet names, Integers are used in zero-indexed
            sheet positions. Specify None to get all sheets.
        :param usecols: int or list, default None
            * If None then parse all columns,
            * If int then indicates last column to be parsed
            * If list of ints then indicates list of column numbers to be parsed
            * If string then indicates comma separated list of Excel column letters and
              column ranges (e.g. "A:E" or "A,C,E:F").  Ranges are inclusive of
              both sides.
        :return:
            a pandas DataFrame
        """

        if not some_sheet_name:
            some_sheet_name = ExcelReader.sheet_names(some_input_path)[0]

        return pd.read_excel(some_input_path,
                             sheet_name=some_sheet_name,
                             error_bad_lines=False,
                             skip_blank_lines=True,
                             skiprows=skiprows,
                             encoding='utf-8',
                             usecols=usecols,
                             names=list(column_names.keys()),
                             dtype=column_names,
                             na_values=['none'])

    @staticmethod
    def drop_columns(a_df: DataFrame,
                     *names) -> None:
        """ drop columns by name
            usecols param in read_excel is buggy hence we have to drop the columns by hand """
        for name in names:
            a_df.drop(name, axis=1, inplace=True)

    @staticmethod
    def sheet_names(some_input_path: str) -> list:
        """ all the sheet names in an excel workbook """
        return pd.ExcelFile(some_input_path).sheet_names
