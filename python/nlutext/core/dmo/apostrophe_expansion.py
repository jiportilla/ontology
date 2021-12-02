#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from base import BaseObject
from base import MandatoryParamError


class ApostropheExpansion(BaseObject):
    """

    """

    def __init__(self,
                 the_input_text: str,
                 is_debug: bool = False):
        """
        Created:
            14-Mar-2017
            craig.trim@ibm.com
            *   used 'expand_enclictics' as a template
        Updated:
            19-Apr-2017
            craig.trim@ibm.com
            *   renamed from "ExpandApostrophes"
        Updated:
            26-Feb-2019
            craig.trim@ibm.com
            *   migrated to -text
        Updated:
            13-Dec-2019
            craig.trim@ibm.com
            *   cleanup params in pursuit of
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1582#issuecomment-16611092
        :param the_input_text:
        """
        BaseObject.__init__(self, __name__)
        if not the_input_text:
            raise MandatoryParamError("Input Text")

        self._is_debug = is_debug
        self._input_text = the_input_text
        self._apostrophes = self.get_apostrophes_dict()

    @staticmethod
    def get_apostrophes_dict() -> list:
        from datadict import the_apostrophes_dict
        return the_apostrophes_dict

    def process(self) -> str:
        normalized = self._input_text.lower()

        for apostrophe in self._apostrophes:
            the_genitive_pattern = u"{0}s".format(apostrophe)
            while the_genitive_pattern in normalized:
                normalized = normalized.replace(the_genitive_pattern, " has")

        if self._is_debug and self._input_text != normalized:
            self.logger.debug("\n".join([
                "Processing Complete",
                f"\tInput: {self._input_text}",
                f"\tOutput: {normalized}"]))

        return normalized
