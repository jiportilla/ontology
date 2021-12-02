#!/usr/bin/env python
# -*- coding: utf-8 -*-

from openpyxl.worksheet.worksheet import Worksheet

COLUMNS = {"A": 20,
           "B": 10,
           "C": 10,
           "D": 10,
           "E": 10,
           "F": 10,
           "G": 10,
           "H": 10,
           "I": 10}


class RankingReportWriter(object):

    def __init__(self,
                 some_excel_worksheet: Worksheet,
                 some_source_dimension: list,
                 some_target_dimensions: list,
                 some_final_ranking: list):
        """
        :param some_excel_worksheet:
            the excel worksheet to write to
        """
        from . import WorksheetHelper

        if not some_excel_worksheet:
            raise ValueError("Mandatory Param: Excel Worksheet")
        if not some_source_dimension:
            raise ValueError("Mandatory Param: Source Dimension")
        if not some_target_dimensions:
            raise ValueError("Mandatory Param: Target Dimemsion")
        if not some_final_ranking:
            raise ValueError("Mandatory Param: Final Ranking")

        self.worksheet = some_excel_worksheet
        self.source_dimension = some_source_dimension
        self.target_dimensions = some_target_dimensions
        self.final_ranking = some_final_ranking
        self.helper = WorksheetHelper

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
        self.worksheet[cell].value = some_text
        self.worksheet[cell].style = some_named_format

    def _write_records(self,
                       source_weights: list,
                       source_values: list):
        """ writes records by row and column """

        def _dimension_value(value: str) -> dict:
            return self.helper.struct(value, "dimension_value_source")

        def _dimension_weight(value: str) -> dict:
            return self.helper.struct(value, "dimension_weight_source")

        def _header_dimension(value: str) -> dict:
            return self.helper.struct(value, "header_dimension")

        def _header_other(value: str) -> dict:
            return self.helper.struct(value, "header_other")

        def _field_key(value: str) -> dict:
            return self.helper.struct(value, "keyfield")

        def _field_weight(value: str) -> dict:
            return self.helper.struct(value, "field_weight_source")

        def _field_rank(value: str) -> dict:
            return self.helper.struct(value, "field_rank")

        d_row_1 = {
            "A1": _header_other("Open Seat ID"),
            "B1": _header_dimension("Cloud"),
            "C1": _header_dimension("Database"),
            "D1": _header_dimension("System Administrator"),
            "E1": _header_dimension("Hard Skill"),
            "F1": _header_dimension("Project Management"),
            "G1": _header_dimension("Service Management"),
            "H1": _header_dimension("Soft Skill"),
            "I1": _header_other("Rank")}

        d_row_2 = {
            "A2": self.helper.struct(self.source_dimension[0]["key_field"],
                                     "keyfield_value_source"),
            "B2": _dimension_value(source_values[0]),
            "C2": _dimension_value(source_values[1]),
            "D2": _dimension_value(source_values[6]),
            "E2": _dimension_value(source_values[2]),
            "F2": _dimension_value(source_values[3]),
            "G2": _dimension_value(source_values[4]),
            "H2": _dimension_value(source_values[5])}

        d_row_3 = {
            "A3": self.helper.struct("Weight",
                                     "dimension_weight_text"),
            "B3": _dimension_weight(source_weights[0]),
            "C3": _dimension_weight(source_weights[1]),
            "D3": _dimension_weight(source_weights[6]),
            "E3": _dimension_weight(source_weights[2]),
            "F3": _dimension_weight(source_weights[3]),
            "G3": _dimension_weight(source_weights[4]),
            "H3": _dimension_weight(source_weights[5])}

        def _field_weight_value(target_dimension: dict,
                                slot_name: str) -> str:
            return target_dimension["slots"][slot_name]["weight"]

        l_values = []
        for i in range(0, len(self.target_dimensions)):
            l_values.append({
                "A{}".format(i + 5): _field_key(
                    self.target_dimensions[i]["key_field"]),
                "B{}".format(i + 5): _field_weight(
                    _field_weight_value(self.target_dimensions[i], "cloud")),
                "C{}".format(i + 5): _field_weight(
                    _field_weight_value(self.target_dimensions[i], "database")),
                "D{}".format(i + 5): _field_weight(
                    _field_weight_value(self.target_dimensions[i], "system administrator")),
                "E{}".format(i + 5): _field_weight(
                    _field_weight_value(self.target_dimensions[i], "hard skill")),
                "F{}".format(i + 5): _field_weight(
                    _field_weight_value(self.target_dimensions[i], "project management")),
                "G{}".format(i + 5): _field_weight(
                    _field_weight_value(self.target_dimensions[i], "service management")),
                "H{}".format(i + 5): _field_weight(
                    _field_weight_value(self.target_dimensions[i], "soft skill")),
                "I{}".format(i + 5): _field_rank(
                    self.final_ranking[i])})

        self.helper.generate(self.worksheet,
                             [d_row_1, d_row_2, d_row_3])
        self.helper.generate(self.worksheet,
                             l_values)

    def process(self):
        """
          Processes the logs from the input directory
          @input: Base directory containing the input and output subdirs.
          @output: None
        """

        def _weights(some_records: list) -> list:
            weights = []
            for record in some_records:
                weights.append([record["slots"][x]["weight"]
                                for x in record["slots"]])
            return weights

        def _values(some_records: list) -> list:
            values = []
            for record in some_records:
                values.append([record["slots"][x]["z_score"]
                               for x in record["slots"]])
            return values

        source_weights = _weights(self.source_dimension)[0]
        source_values = _values(self.source_dimension)[0]

        self.helper.column_widths(self.worksheet,
                                  COLUMNS)

        self._write_records(source_weights,
                            source_values)
