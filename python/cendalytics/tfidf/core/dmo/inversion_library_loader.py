#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import os
import time

import pandas as pd
from pandas import DataFrame

from base import BaseObject
from base import FileIO


class InversionLibraryLoader(BaseObject):
    """ Load Inversion Library """

    __df_inversion = None

    __header_row = ["Id",
                    "KeyField",
                    "Term"]

    def __init__(self,
                 library_name: str,
                 is_debug: bool = False):
        """
        Created:
            5-Nov-2019
            craig.trim@ibm.com
            *   based on 'vectorspace-library-loader'
        :param library_name:
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)
        self.is_debug = is_debug
        self._library_name = library_name

    def _library_path(self):
        fname = f"resources/confidential_input/vectorspace/{self._library_name}"
        return os.path.join(os.environ["GTS_BASE"],
                            fname)

    def _process(self) -> None:
        start = time.time()

        _input_path = FileIO.absolute_path(self._library_path())

        _data_types = dict((key, str) for (key) in self.__header_row)

        df = pd.read_csv(
            _input_path,
            delim_whitespace=False,
            sep='\t',
            error_bad_lines=False,
            skip_blank_lines=True,
            skiprows=1,
            comment='#',
            encoding='utf-8',
            names=self.__header_row,
            dtype=_data_types,
            na_values=['none'],
            usecols=self.__header_row)

        df.fillna(value='', inplace=True)

        end = time.time()
        if self.is_debug:
            self.logger.debug("\n".join([
                "Read CSV File",
                "\tPath: {}".format(_input_path),
                "\tTotal Time: {}".format(end - start)]))

        self.__df_inversion = df

    def df(self) -> DataFrame:
        if self.__df_inversion is None:
            self._process()
            
        return self.__df_inversion
