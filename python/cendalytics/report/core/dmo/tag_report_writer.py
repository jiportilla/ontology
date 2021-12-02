#!/usr/bin/env python
# -*- coding: utf-8 -*-


from openpyxl.worksheet.worksheet import Worksheet

from base import BaseObject
from datadict import FindDimensions
from datadict import FindRelationships

COLUMNS = {"A": 25,
           "B": 25,
           "C": 45,
           "D": 45,
           "E": 25,
           "F": 25,
           "G": 25,
           "H": 25,
           "I": 25}


class TagReportWriter(BaseObject):

    def __init__(self,
                 key_field: str,
                 excel_worksheet: Worksheet,
                 tag_record: dict,
                 is_debug: bool = False):
        """
        Created:
            12-Nov-2019
            craig.trim@ibm.com
            *   based on 'feedback-report-writer'
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1331#issuecomment-15953128
        :param key_field:
            the value of the keyfield to process
            (e.g. the actual Serial Number or Open Seat ID)
        :param excel_worksheet:
            the excel worksheet to write to
        :param tag_record:
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)
        from . import WorksheetHelper

        self._is_debug = is_debug
        self._key_field = key_field
        self._tag_record = tag_record
        self._excel_worksheet = excel_worksheet

        self._helper = WorksheetHelper
        self._rel_finder = FindRelationships()
        self._supply_finder = FindDimensions("supply")
        self._learning_finder = FindDimensions("learning")

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

    def _parent_hierarchy(self,
                          tag: str) -> str:
        """
        Purpose:
            Show the entity ancestry back to root
            this is useful for demonstrating the tag's context in the Ontology
        Example 1:
            Input:      'W3C standard'
            Output:     'standard, entity'
        Example 2:
            Input:      'deep learning'
            Output:     'cognitive skill, technical skill, skill'
                        'statistical algorithm, technical skill, skill'
        :param tag:
            any input tag
        :return:
            a string value representing the ancestry of the input tag
        """
        return '\n'.join([', '.join(x) for x in self._rel_finder.ancestors(tag)])

    def _supply_schema(self,
                       tag: str) -> str:
        """
        Purpose:
            Find the associated supply schema elements
        :param tag:
            any tag from a mongoDB field
        :return:
            the result as a comma-separated string
        """
        return ', '.join(self._supply_finder.find(tag))

    def _learning_schema(self,
                         tag: str) -> str:
        """
        Purpose:
            Find the associated learning schema elements
        :param tag:
            any tag from a mongoDB field
        :return:
            the result as a comma-separated string
        """
        return ', '.join(self._learning_finder.find(tag))

    @staticmethod
    def _field_data(a_field: dict) -> str:
        """
        Purpose:
            Format a field meta-data expression
        Format:
            Field Name (Field Type)
        Sample Output:
            position_description (long-text)
        :param a_field:
            any field of a mongoDB record
        :return:
            the formatted text
        """
        return f"{a_field['name']} ({a_field['type']})\n{a_field['field_id']}"

    @staticmethod
    def _original_text(a_field: dict) -> str:
        """
        Purpose:
            Return the original text as a string
        :param a_field:
            any field of a mongoDB record
        :return:
            the normalized text
        """
        if type(a_field["value"]) == list:
            return " ".join([str(x) for x in a_field["value"]])
        return a_field["value"]

    def _normalized_text(self,
                         a_field: dict) -> str:
        """
        Purpose:
            Conditionally return the normalized text
        Conditions
            1. Normalized Text must exist
            2. Normalized Text must not be equivalent to Original Text
        :param a_field:
            any field of a mongoDB record
        :return:
            the normalized text
        """
        if "normalized" not in a_field:
            return ""

        normalized_text = " ".join(a_field["normalized"])
        if normalized_text == self._original_text(a_field):
            return ""

        return normalized_text

    @staticmethod
    def _tags(a_field: dict) -> dict:
        """
        Purpose:
            Key Tags by Confidence Level in descending order
        Sample Output:
            {   91.5: [ 'artificial neural network'],
                78.2: [ 'deploy',
                        'google',
                        'tensorflow'],
                88.2: [ 'team lead'] }
        :param a_field:
            any field of a mongoDB record
        :return:
            a dictionary keyed by Confidence levrel
        """
        d = {}
        for x in a_field["tags"]["supervised"]:
            if x[1] not in d:
                d[x[1]] = []
            d[x[1]].append(x[0])
        return d

    def _write_records(self):
        """ writes records by row and column """

        def _header_other(value: str) -> dict:
            return self._helper.struct(value, "header_other")

        def _collection_name(value: str) -> dict:
            return self._helper.struct(value, "collection_name")

        def _collection_value(value: str) -> dict:
            return self._helper.struct(value, "collection_value")

        def _collection_text(value: str) -> dict:
            return self._helper.struct(value, "collection_text")

        d_row_1 = {
            "A1": _header_other(self._tag_record["key_field"]),
            "B1": _header_other("Field MetaData"),
            "C1": _header_other("Original"),
            "D1": _header_other("Normalized"),
            "E1": _header_other("Tag"),
            "F1": _header_other("Confidence"),
            "G1": _header_other("Parents"),
            "H1": _header_other(f"Supply Schema"),
            "I1": _header_other(f"Learning Schema")}

        row = 2
        l_structs = []

        for field in self._tag_record["fields"]:

            field_data = self._field_data(a_field=field)
            original_text = self._original_text(a_field=field)
            normalized_text = self._normalized_text(a_field=field)

            if "tags" in field:
                tag_ctr = 1

                d_tags = self._tags(a_field=field)
                for tag_confidence in sorted(d_tags.keys()):
                    for tag_name in d_tags[tag_confidence]:

                        parents = self._parent_hierarchy(tag_name)
                        supply_schema = self._supply_schema(tag_name)
                        learning_schema = self._learning_schema(tag_name)

                        if tag_ctr == 1:
                            l_structs.append({
                                "A{}".format(row): _collection_value(field["collection"]["name"]),
                                "B{}".format(row): _collection_value(field_data),
                                "C{}".format(row): _collection_text(original_text),
                                "D{}".format(row): _collection_text(normalized_text),
                                "E{}".format(row): _collection_value(tag_name),
                                "F{}".format(row): _collection_value(tag_confidence),
                                "G{}".format(row): _collection_value(parents),
                                "H{}".format(row): _collection_value(supply_schema),
                                "I{}".format(row): _collection_value(learning_schema)})
                        elif tag_ctr > 1:
                            l_structs.append({
                                "A{}".format(row): _collection_text(""),
                                "B{}".format(row): _collection_text(""),
                                "C{}".format(row): _collection_text(""),
                                "D{}".format(row): _collection_text(""),
                                "E{}".format(row): _collection_value(tag_name),
                                "F{}".format(row): _collection_value(tag_confidence),
                                "G{}".format(row): _collection_value(parents),
                                "H{}".format(row): _collection_value(supply_schema),
                                "I{}".format(row): _collection_value(learning_schema)})
                        tag_ctr += 1
                        row += 1
            else:
                l_structs.append({
                    "A{}".format(row): _collection_name(field["collection"]["name"]),
                    "B{}".format(row): _collection_text(original_text),
                    "C{}".format(row): _collection_text(normalized_text),
                    "D{}".format(row): _collection_text(""),
                    "E{}".format(row): _collection_text(""),
                    "F{}".format(row): _collection_text(""),
                    "G{}".format(row): _collection_text(""),
                    "H{}".format(row): _collection_text(""),
                    "I{}".format(row): _collection_text("")})

        self._helper.generate(worksheet=self._excel_worksheet,
                              l_structs=[d_row_1])

        self._helper.generate(worksheet=self._excel_worksheet,
                              l_structs=l_structs)

    def process(self):
        self._helper.column_widths(worksheet=self._excel_worksheet,
                                   d_columns=COLUMNS)

        self._write_records()
