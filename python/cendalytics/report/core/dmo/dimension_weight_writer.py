#!/usr/bin/env python
# -*- coding: utf-8 -*-


from openpyxl.worksheet.worksheet import Worksheet

from base import BaseObject
from datadict import FindDimensions


class DimensionWeightWriter(BaseObject):

    def __init__(self,
                 xdm_record: dict,
                 xdm_schema: str,
                 excel_worksheet: Worksheet,
                 is_debug: bool = False):
        """
        Updated:
            15-Oct-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1331#issuecomment-16034968
        :param excel_worksheet:
            the excel worksheet to write to
        :param xdm_record:
        """
        BaseObject.__init__(self, __name__)
        from . import WorksheetHelper

        self._is_debug = is_debug
        self._xdm_record = xdm_record
        self._excel_worksheet = excel_worksheet

        self._helper = WorksheetHelper

        self._column_letters = self._helper.column_letters()
        self._dim_finder = FindDimensions(schema=xdm_schema,
                                          is_debug=self._is_debug)

    def _define_column_widths(self):
        d_columns = {"A": 20}

        ctr = 1
        for _ in self._dim_finder.top_level_entities():
            d_columns[self._column_letters[ctr]] = 8
            ctr += 1

        self._helper.column_widths(worksheet=self._excel_worksheet,
                                   d_columns=d_columns)

    def process(self):
        """

        :return:
        """
        self._define_column_widths()

        def _header_other(value: str) -> dict:
            return self._helper.struct(value, "header_other")

        def _header_dimension(value: str) -> dict:
            return self._helper.struct(value, "header_dimension")

        def _dimension_weight_source(value: str) -> dict:
            return self._helper.struct(value, "dimension_weight_source")

        rows = []

        slots = sorted(self._xdm_record["slots"].keys())

        ctr = 1
        for slot in slots:
            rows.append({f"{self._column_letters[ctr]}1": _header_dimension(slot)})
            ctr += 1

        ctr = 1
        rows.append({"A2": _header_other("Weight")})
        for slot in slots:
            weight = [self._xdm_record["slots"][x]["weight"]
                      for x in self._xdm_record["slots"] if x == slot][0]
            rows.append({f"{self._column_letters[ctr]}2": _dimension_weight_source(weight)})
            ctr += 1

        ctr = 1
        rows.append({"A3": _header_other("z-Score")})
        for slot in slots:
            weight = [self._xdm_record["slots"][x]["z_score"]
                      for x in self._xdm_record["slots"] if x == slot][0]
            rows.append({f"{self._column_letters[ctr]}3": _dimension_weight_source(weight)})
            ctr += 1

        ctr = 1
        rows.append({"A4": _header_other("Norm")})
        for slot in slots:
            weight = [self._xdm_record["slots"][x]["z_score_norm"]
                      for x in self._xdm_record["slots"] if x == slot][0]
            rows.append({f"{self._column_letters[ctr]}4": _dimension_weight_source(weight)})
            ctr += 1

        self._helper.generate(worksheet=self._excel_worksheet,
                              l_structs=rows)
