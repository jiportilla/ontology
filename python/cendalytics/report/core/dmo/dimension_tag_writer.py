#!/usr/bin/env python
# -*- coding: utf-8 -*-


from openpyxl.worksheet.worksheet import Worksheet

from base import BaseObject
from datadict import FindDimensions

COLUMNS = {"A": 20,
           "B": 15,
           "C": 15,
           "D": 15,
           "E": 15,
           "F": 15,
           "G": 15,
           "H": 15,
           "I": 15,
           "J": 15,
           "K": 40,
           "L": 40,
           "M": 20}


class DimensionTagWriter(BaseObject):

    def __init__(self,
                 key_field: str,
                 excel_worksheet: Worksheet,
                 tag_record: dict,
                 xdm_record: dict,
                 xdm_schema: str = 'supply',
                 is_debug: bool = False):
        """
        Updated:
            29-Oct-2019
            craig.trim@ibm.com
            *   remove 'entity-schema-finder' in favor of new approach
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/916#issuecomment-15620801
        Updated:
            12-Nov-2019
            craig.trim@ibm.com
            *   dust-off and refactor in pursuit of
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1331
        Updated:
            15-Nov-2019
            craig.trim@ibm.com
            *   renamed from 'feedback-report-writer' in pursuit of
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1331#issuecomment-16027118
        :param excel_worksheet:
            the excel worksheet to write to
        :param tag_record:
        :param xdm_record:
        :param key_field:
            the value of the keyfield to process
            (e.g. the actual Serial Number or Open Seat ID)
        :param xdm_schema:
            the name of the schema to perform the type lookup
            Notes:
            -   typically either 'supply' or 'learning'
            -   the full list is on path 'resources/config/dimensionality'
        """
        BaseObject.__init__(self, __name__)
        from . import WorksheetHelper

        self._is_debug = is_debug
        self._key_field = key_field
        self._tag_record = tag_record
        self._xdm_record = xdm_record
        self._excel_worksheet = excel_worksheet

        self._helper = WorksheetHelper
        self._dim_finder = FindDimensions(xdm_schema)

    def _write_value(self,
                     some_column: str,
                     some_row: int,
                     some_text: str,
                     some_named_format: str):
        """
        :param some_column:
        :param some_row:
        :param some_text:
        :param some_named_format:
        """
        cell = "{}{}".format(some_column,
                             some_row)
        self._excel_worksheet[cell].value = some_text
        self._excel_worksheet[cell].style = some_named_format

    def _write_records(self,
                       xdm_values: list):
        """ writes records by row and column """

        def _header_dimension(value: str) -> dict:
            return self._helper.struct(value, "header_dimension")

        def _header_other(value: str) -> dict:
            return self._helper.struct(value, "header_other")

        def _dimension_weight_text(value: str) -> dict:
            return self._helper.struct(value, "dimension_weight_text")

        def _dimension_weight_source(value: str) -> dict:
            return self._helper.struct(value, "dimension_weight_source")

        def _collection_name(value: str) -> dict:
            return self._helper.struct(value, "collection_name")

        def _collection_value(value: str) -> dict:
            return self._helper.struct(value, "collection_value")

        def _collection_text(value: str) -> dict:
            return self._helper.struct(value, "collection_text")

        d_row_1 = {
            "A1": _header_other(self._tag_record["key_field"]),
            "B1": _header_dimension("Blockchain"),
            "C1": _header_dimension("Quantum"),
            "D1": _header_dimension("Cloud"),
            "E1": _header_dimension("Database"),
            "F1": _header_dimension("System Administrator"),
            "G1": _header_dimension("Hard Skill"),
            "H1": _header_dimension("Project Management"),
            "I1": _header_dimension("Service Management"),
            "J1": _header_dimension("Soft Skill"),
            "K1": _header_other("Original"),
            "L1": _header_other("Normalized"),
            "M1": _header_other("Field Type")}

        d_row_2 = {
            "A2": _dimension_weight_text(""),
            "B2": _dimension_weight_source(xdm_values[0]),  # Blockchain
            "C2": _dimension_weight_source(xdm_values[0]),  # Quantum
            "D2": _dimension_weight_source(xdm_values[0]),  # Cloud
            "E2": _dimension_weight_source(xdm_values[1]),  # Database
            "F2": _dimension_weight_source(xdm_values[6]),  # System Administrator
            "G2": _dimension_weight_source(xdm_values[2]),  # Hard Skill
            "H2": _dimension_weight_source(xdm_values[3]),  # Project Management
            "I2": _dimension_weight_source(xdm_values[4]),  # Service Management
            "J2": _dimension_weight_source(xdm_values[5])}  # Soft Skill

        row = 3
        l_structs = []

        fields = [field for field in self._tag_record["fields"]
                  if "tags" in field]

        for field in fields:
            d_schema = {}

            tags = [tag[0] for tag in field["tags"]["supervised"]
                    if tag[1] > 65]

            for tag in tags:
                for parent in self._dim_finder.find(tag):
                    if parent not in d_schema:
                        d_schema[parent] = []
                    d_schema[parent].append(tag)

            def _tags(a_parent: str) -> str:
                if a_parent not in d_schema:
                    return ""
                return ",\n".join(d_schema[a_parent])

            def _original_text() -> str:
                return " ".join(field["value"])

            def _normalized_text() -> str:
                normalized_text = " ".join(field["normalized"])

                if normalized_text == _original_text():
                    return ""

                return normalized_text

            def _field_type() -> str:
                return field["type"]

            l_structs.append({
                "A{}".format(row): _collection_name(field["collection"]["name"]),
                "B{}".format(row): _collection_value(_tags("blockchain")),
                "C{}".format(row): _collection_value(_tags("quantum")),
                "D{}".format(row): _collection_value(_tags("cloud")),
                "E{}".format(row): _collection_value(_tags("database")),
                "F{}".format(row): _collection_value(_tags("system administrator")),
                "G{}".format(row): _collection_value(_tags("hard skill")),
                "H{}".format(row): _collection_value(_tags("project management")),
                "I{}".format(row): _collection_value(_tags("service management")),
                "J{}".format(row): _collection_value(_tags("soft skill")),
                "K{}".format(row): _collection_text(_original_text()),
                "L{}".format(row): _collection_text(_normalized_text()),
                "M{}".format(row): _collection_value(_field_type())})

            row += 1

        self._helper.generate(worksheet=self._excel_worksheet,
                              l_structs=[d_row_1, d_row_2])

        self._helper.generate(worksheet=self._excel_worksheet,
                              l_structs=l_structs)

    def _xdm_values(self):
        def _values(some_records: list) -> list:
            values = []
            for record in some_records:
                values.append([record["slots"][x]["weight"]
                               for x in record["slots"]])
            return values

        return _values([self._xdm_record])[0]

    def process(self):
        self._helper.column_widths(worksheet=self._excel_worksheet,
                                   d_columns=COLUMNS)

        xdm_values = self._xdm_values()
        self._write_records(xdm_values=xdm_values)
