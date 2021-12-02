#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from base import FileIO


class FieldTextWeight(BaseObject):

    def __init__(self,
                 is_debug: bool = False):
        """
        Created:
            8-May-2019
            craig.trim@ibm.com
            *   refactored out of 'generate-dataframe-weights'
        Updated:
            17-Oct-2019
            craig.trim@ibm.com
            *   rename from 'badge-name-weight' and generalize to all field text
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1142#issuecomment-15374359
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._weights = self._weight_config()

    @staticmethod
    def _weight_config():
        d = {}

        def _doc() -> dict:
            return FileIO.file_to_yaml_by_relative_path(
                "resources/config/weights/field_text_weights.yml")

        for phrase in _doc()["text"]:
            d[phrase["phrase"]] = phrase["weight"]

        return d

    def process(self,
                field_text: str) -> float:
        for phrase in self._weights:
            if phrase in field_text:
                return self._weights[phrase]
        return 1.0
