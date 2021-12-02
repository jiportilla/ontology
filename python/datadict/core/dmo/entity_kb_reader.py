#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import logging
import time

import pandas as pd
from base import BaseObject

logger = logging.getLogger(__name__)


class EntityKbReader(BaseObject):
    """
        Created:
            14-Sept-2016
            craig.trim@ibm.com
            *   prior to this was using hard-coded dicts
        Updated:
            7-Oct-2016
            craig.trim@ibm.com
            *   added to_csv method
            *   changed init params
        Updated:
            3-Jul-2017
            craig.trim@ibm.com
            *   added 'children' and 'parent'
    """

    def __init__(self, some_input_file):
        BaseObject.__init__(self, __name__)
        self.input_file = some_input_file

    @staticmethod
    def get_header_row():
        return [
            'label',
            'type',
            'param',
            'scope',
            'variations',
            'children',
            'parent',
            'prov']

    @staticmethod
    def get_data_types():
        return {
            'label': str,
            'type': str,
            'param': str,
            'scope': str,
            'variations': str,
            'children': str,
            'parent': str,
            'prov': str}

    def read_csv(self):
        start = time.time()
        df = pd.read_csv(
            self.input_file,
            delim_whitespace=False,
            sep='|',
            error_bad_lines=False,
            skip_blank_lines=True,
            skiprows=0,
            comment='#',
            encoding='latin-1',
            names=self.get_header_row(),
            dtype=self.get_data_types(),
            na_values=['none'],
            usecols=self.get_header_row())

        df.fillna(value='', inplace=True)

        end = time.time()
        logger.info('Read CSV File\n\tpath = {}\n\telapsed-time = {}'.format(self.input_file, (end - start)))

        return df

    @staticmethod
    def to_csv(some_modified_df, some_output_file):
        start = time.time()

        some_modified_df.fillna(value='', inplace=True)
        some_modified_df.to_csv(
            some_output_file,
            sep='|',
            delim_whitespace=False,
            error_bad_lines=False,
            skip_blank_lines=True,
            index=False,  # Write row names (index), default=True
            header=False,  # Write out column names, default=True
            mode='w',  # Python write mode, default ‘w’
            encoding='utf8')  # defaults to ‘ascii’ on Python 2 and ‘utf-8’ on Python 3.

        end = time.time()
        logger.info('Wrote CSV File\n\tpath = {}\n\telapsed-time = {}'.format(some_output_file, (end - start)))
