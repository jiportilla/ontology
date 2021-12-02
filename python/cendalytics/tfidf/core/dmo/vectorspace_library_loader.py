#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import os
import time

import pandas as pd
from pandas import DataFrame

from base import BaseObject
from base import FileIO


class VectorSpaceLibraryLoader(BaseObject):
    """ Load Vector Space """

    __df_vectorspace = None

    __columns = {"Number": int,
                 "Doc": str,
                 "DocsWithTerm": int,
                 "IDF": float,
                 "TF": float,
                 "TFIDF": float,
                 "Term": str,
                 "TermFrequencyInCorpus": int,
                 "TermFrequencyInDoc": int,
                 "TermsInDoc": int,
                 "TotalDocs": int}

    def __init__(self,
                 library_name: str,
                 is_debug: bool = False):
        """
        Created:
            10-Jul-2019
            craig.trim@ibm.com
            *   search the skills vector space
        Updated:
            5-Nov-2019
            craig.trim@ibm.com
            *   renamed from 'skills-vectorspace-loader' and
                refactored out of nlusvc project
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1261
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


        df = pd.read_csv(
            _input_path,
            delim_whitespace=False,
            sep='\t',
            error_bad_lines=False,
            skip_blank_lines=True,
            skiprows=1,
            comment='#',
            encoding='utf-8',
            names=self.__columns.keys(),
            dtype=self.__columns,
            na_values=['none'],
            usecols=self.__columns.keys())

        df.fillna(value='', inplace=True)

        end = time.time()
        if self.is_debug:
            self.logger.debug("\n".join([
                "Read CSV File",
                "\tPath: {}".format(_input_path),
                "\tTotal Time: {}".format(end - start)]))

        self.__df_vectorspace = df

    def df(self) -> DataFrame:
        if self.__df_vectorspace is None:
            self._process()

        return self.__df_vectorspace
