#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from base import MandatoryParamError


class EnclicticExpansion(BaseObject):

    def __init__(self,
                 the_input_text: str,
                 is_debug: bool = False):
        """
        Updated:
            12-Feb-2017
            craig.trim@ibm.com
            *   added "`" to apostrophe array
            *   changed final logging statement to DEBUG level
        Updated:
            19-Apr-2017
            craig.trim@ibm.com
            *   renamed from 'ExpandEnclictics'
        Updated:
            26-Feb-2019
            craig.trim@ibm.com
            *   migrated to text
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

        self._input_text = the_input_text
        self._is_debug = is_debug
        self._enclictics = self.get_enclictics_dict()
        self._apostrophes = self.get_apostrophes_dict()

    @staticmethod
    def get_enclictics_dict() -> dict:
        from datadict import the_enclitics_dict
        return the_enclitics_dict

    @staticmethod
    def get_apostrophes_dict() -> list:
        from datadict import the_apostrophes_dict
        return the_apostrophes_dict

    def process(self) -> str:
        normalized = self._input_text

        for key in self._enclictics:
            for value in self._enclictics[key]:

                for apostrophe in self._apostrophes:
                    variant = value.format(apostrophe)

                    value_1 = u"{} ".format(variant)
                    value_2 = u" {}".format(variant)

                    if value_1 in normalized:
                        normalized = normalized.replace(value_1, u"{} ".format(key))

                    if normalized.endswith(value_2):
                        normalized = normalized.replace(value_2, u" {}".format(key))

        if self._is_debug and self._input_text != normalized:
            self.logger.debug("\n".join([
                "Enclictic Expansion Complete",
                f"\tInput: {self._input_text}",
                f"\tOutput: {normalized}"]))

        return normalized
