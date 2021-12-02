#!/usr/bin/env python
# -*- coding: utf-8 -*-


from openpyxl.worksheet.worksheet import Worksheet

from base import BaseObject
from cendantdim import TimeDimensionCalculator

COLUMNS = {"A": 20,
           "B": 20,
           "C": 20,
           "D": 20}


class TimeDimensionWriter(BaseObject):

    def __init__(self,
                 tag_record: dict,
                 excel_worksheet: Worksheet,
                 is_debug: bool = False):
        """
        Created:
            15-Nov-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1331#issuecomment-16034502
        :param excel_worksheet:
            the excel worksheet to write to
        :param tag_record:
        """
        BaseObject.__init__(self, __name__)
        from . import WorksheetHelper

        self._is_debug = is_debug
        self._tag_record = tag_record
        self._excel_worksheet = excel_worksheet

        self._helper = WorksheetHelper

    def process(self):
        def _header_other(value: str) -> dict:
            return self._helper.struct(value, "header_other")

        def _collection_value(value: str) -> dict:
            return self._helper.struct(value, "collection_value")

        self._helper.column_widths(worksheet=self._excel_worksheet,
                                   d_columns=COLUMNS)

        result = TimeDimensionCalculator(is_debug=True,
                                         source_record=self._tag_record).process()

        rows = [{
            "A1": _header_other("Key"),
            "B1": _header_other("Weight"),
            "C1": _header_other("zScore"),
            "D1": _header_other("zScoreNorm")}]

        row_ctr = 2
        for _, row in result.final().iterrows():
            rows.append({
                f"A{row_ctr}": _collection_value(row['Schema']),
                f"B{row_ctr}": _collection_value(row['Weight']),
                f"C{row_ctr}": _collection_value(row['zScore']),
                f"D{row_ctr}": _collection_value(row['zScoreNorm'])})
            row_ctr += 1

        row_ctr += 1

        rows.append({
            f"A{row_ctr}": _header_other("Start Year"),
            f"B{row_ctr}": _header_other("End Year")})

        for d_years in result.career_history():
            row_ctr += 1
            rows.append({
                f"A{row_ctr}": _collection_value(d_years["start"]),
                f"B{row_ctr}": _collection_value(d_years["end"])})

        row_ctr += 1

        rows.append({
            f"A{row_ctr}": _header_other("Start Year")})

        row_ctr += 1

        rows.append({
            f"A{row_ctr}": _collection_value(result.start_year())})

        self._helper.generate(worksheet=self._excel_worksheet,
                              l_structs=rows)
