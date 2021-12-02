#!/usr/bin/env python
# -*- coding: utf-8 -*-


import pandas as pd
from openpyxl.workbook.workbook import Workbook
from pandas.io.excel import ExcelWriter

from base import BaseObject


class ExcelWriterInitializer(BaseObject):

    def __init__(self,
                 tabs: list,
                 file_path: str,
                 is_debug: bool = False):
        """
        Created:
            13-Nov-2019
            craig.trim@ibm.com
            *   refactored out of 'generate-feedback-report'
        :param tabs:
            list        a list of tabs for the Excel Spreadsheet
        :param file_path:
            str         the output file path
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)

        self._tabs = tabs
        self._is_debug = is_debug
        self._file_path = file_path

    @staticmethod
    def _define_formats(workbook: Workbook):
        from . import FormatGenerator
        relative_path = "resources/config/reporting/feedback_report_format.yml"
        FormatGenerator(workbook,
                        relative_path).process()

    def _create_worksheets(self,
                           writer: ExcelWriter) -> None:
        """
        Purpose:
            Generate a worksheet using an empty dataframe (pandas workaround)
        :param writer:
            an Excel writer
        :return:
            an Excel Worksheet
        """
        df = pd.DataFrame(index=[], columns=[])
        [df.to_excel(writer, tab, index=False) for tab in self._tabs]

    def _writer(self) -> ExcelWriter:
        """ generate the workbook at the defined path """
        if self._is_debug:
            self.logger.debug(f"Output File Path:\n\t{self._file_path}")

        return pd.ExcelWriter(self._file_path)

    def process(self) -> ExcelWriter:
        writer = self._writer()

        self._define_formats(writer.book)
        self._create_worksheets(writer)

        return writer
