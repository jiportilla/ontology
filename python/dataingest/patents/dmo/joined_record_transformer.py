#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import math

import pandas as pd

from base import BaseObject
from base import FieldStructure


class JoinedRecordTransformer(BaseObject):
    """ Transforms a "Joined" Patent Record into a "Source" Patent Record
        A "joined" Record is a Row in a DataFrame that has merged
            USPTO + BluePages Patent Information
        A "source" Record is a single entry within
            the 'patents_src_<date>' collection
    """

    def __init__(self,
                 row: pd.Series,
                 is_debug: bool = False):
        """
        Created:
            24-Oct-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/-cdo/unstructured-analytics/issues/1196
        """
        BaseObject.__init__(self, __name__)

        self._row = row
        self._is_debug = is_debug

    @staticmethod
    def _field(field_name: str,
               field_value: str,
               field_type="text",
               transformations=None) -> dict or None:

        def cleanse() -> str or int or None:
            if not field_value:
                return None
            if (type(field_value) == int or type(field_value) == float) and math.isnan(field_value):
                return None
            if field_type == "int":
                return int(field_value)

            return field_value.strip()

        field_value = cleanse()
        if not field_value:
            return None

        return FieldStructure.generate_src_field(agent_name="System",
                                                 field_type=field_type,
                                                 field_name=field_name,
                                                 field_value=field_value,
                                                 transformations=transformations)

    def process(self) -> dict:
        fields = [
            self._field(field_name="patent_title",
                        field_value=self._row["Title"],
                        field_type="long-text"),
            self._field(field_name="patent_abstract",
                        field_value=self._row["Abstract"],
                        field_type="long-text"),
            self._field(field_name="patent_id",
                        field_value=self._row["ID"],
                        field_type="text"),
            self._field(field_name="patent_country",
                        field_value=self._row["Country"],
                        field_type="text"),
            self._field(field_name="patent_date",
                        field_value=self._row["Date"],
                        field_type="text"),
            self._field(field_name="patent_filename",
                        field_value=self._row["Filename"],
                        field_type="text"),
            self._field(field_name="patent_kind",
                        field_value=self._row["Kind"],
                        field_type="text"),
            self._field(field_name="patent_number_of_claims",
                        field_value=self._row["NumberOfClaims"],
                        field_type="int"),
            self._field(field_name="patent_type",
                        field_value=self._row["Type"],
                        field_type="text"),
            self._field(field_name="patent_withdrawn",
                        field_value=self._row["Withdrawn"],
                        field_type="int")]

        fields = [x for x in fields if x]

        return {
            "fields": fields,
            "key_field": self._row["SerialNumber"],
            "manifest": {
                "name": "patents"}}
