#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import logging
import time

import pandas as pd

from base import BaseObject

logger = logging.getLogger(__name__)


class SiblingsKbReader(BaseObject):
    """
        Created:
            9-Nov-2016
            craig.trim@ibm.com
            *   prior to this was using hard-coded dicts
            *   logic for siblings was also spread throughout the system
    """

    def __init__(self, some_input_file):
        BaseObject.__init__(self, __name__)
        self.input_file = some_input_file

    @staticmethod
    def get_header_row():
        return [
            'name',
            'members']

    @staticmethod
    def get_data_types():
        return {
            'name': str,
            'members': str}

    def read_csv(self, show_dtypes=False):
        start = time.time()
        df = pd.read_csv(
            self.input_file,
            delim_whitespace=False,
            sep='|',
            error_bad_lines=False,
            skip_blank_lines=True,
            skiprows=0,
            comment='#',
            encoding='utf-8',
            names=self.get_header_row(),
            dtype=self.get_data_types(),
            na_values=['none'],
            usecols=self.get_header_row())

        df.fillna(value='', inplace=True)

        end = time.time()
        logger.info('Read CSV File\n\tpath = {}\n\telapsed-time = {}'.format(self.input_file, (end - start)))

        return df
