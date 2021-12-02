#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import os
import time

import pandas as pd

from base import BaseObject
from base import FileIO


class SynonymsKbReader(BaseObject):

    @staticmethod
    def by_name(ontology_name: str,
                is_debug: bool = True):
        def relative_path() -> str:
            config_path = os.path.join(os.environ["CODE_BASE"],
                                       "resources/config/config.yml")
            doc = FileIO.file_to_yaml(config_path)
            return doc["synonyms"][ontology_name]["path"]

        input_path = FileIO.absolute_path(file_name=relative_path(),
                                          validate=True)

        return SynonymsKbReader(is_debug=is_debug,
                                some_input_file=input_path)

    def __init__(self,
                 some_input_file: str,
                 is_debug: bool = False):
        """
        Created:
            1-Nov-2016
            craig.trim@ibm.com
            *   prior to this was using hard-coded dicts
        Updated:
            21-Feb-2019
            craig.trim@ibm.com
            *   migrated to text
        Updated:
            13-Dec-2019
            craig.trim@ibm.com
            *   add 'cendant' static method for initialization in pursuit of
                https://github.ibm.com/GTS-CDO/unstructured-analytics/pull/1587
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._input_file = some_input_file

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Instantiate SynonymsKbReader",
                f"\tInput File: {some_input_file}"]))

    @staticmethod
    def get_header_row():
        return [
            'canon',
            'variants']

    @staticmethod
    def get_data_types():
        return {
            'canon': str,
            'variants': str}

    def read_csv(self):
        start = time.time()

        df = pd.read_csv(
            self._input_file,
            delim_whitespace=False,
            sep='~',
            error_bad_lines=False,
            skip_blank_lines=True,
            skiprows=0,
            encoding='utf-8',
            names=self.get_header_row(),
            dtype=self.get_data_types(),
            na_values=['none'],
            usecols=self.get_header_row())

        df.fillna(value='', inplace=True)
        end = time.time()

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Read CSV File",
                f"\tTotal Time: {end - start}",
                f"\tInput File: {self._input_file}"]))

        return df
