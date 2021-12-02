# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from base import MandatoryParamError


class TextNormalizer(BaseObject):
    """ Text Normalizer """

    def __init__(self,
                 some_input_text: str,
                 is_debug: bool = False):
        """
        Created:
            17-Feb-2017
            craig.trim@ibm.com
            *   was simple method in "normalize_incoming_text.py"
            *   added additional function (" gb" => "gb")
        Updated:
            24-Feb-2017
            craig.trim@ibm.com
            *   updated 'punctuation_replacements' with parens
        Updated:
            17-Mar-2017
            craig.trim@ibm.com
            *   updated 'punctuation_replacements' with hyphen
        Updated:
            21-Feb-2019
            craig.trim@ibm.com
            *   migrated from text
        Updated:
            21-Mar-2019
            craig.trim@ibm.com
            *   use d-replace dict instead of line-by-linen replacement
        :param some_input_text:
        """
        BaseObject.__init__(self, __name__)
        if not some_input_text:
            raise MandatoryParamError("Input Text")

        self._is_debug = is_debug
        self._input_text = some_input_text

    @staticmethod
    def punctuation_replacements(some_input_text: str) -> str:
        """
        perform any punctuation replacement
        :param some_input_text:
        :return: normalized text
        """
        d_replace = {
            '/': ' ',
            '(': ' ',
            ')': ' ',
            '-': ' ',
            ':': ' : ',
            '“': ' ',
            '–': ' ',
            '—': ' ',
            '"': ' ',
            ',': ' ',
            '®': ' ',
            '•': ' ',
            '...': ' ',
            '・': ' '}

        # these patterns don't work in a dictionary for some reason
        some_input_text = some_input_text.replace('\xad', '')
        some_input_text = some_input_text.replace('\u00ad', '')
        some_input_text = some_input_text.replace('\N{SOFT HYPHEN}', '')

        # perform dictionary replacements
        for k in d_replace:
            if k in some_input_text:
                some_input_text = some_input_text.replace(k, d_replace[k])

        while "  " in some_input_text:
            some_input_text = some_input_text.replace("  ", " ")

        # perform character+token replacements (externalize?)
        some_input_text = some_input_text.replace('a: drive', "adrive")
        some_input_text = some_input_text.replace('b: drive', "bdrive")
        some_input_text = some_input_text.replace('c: drive', "cdrive")

        return some_input_text

    def process(self) -> str:
        normalized = self._input_text.lower().strip()
        normalized = self.punctuation_replacements(normalized)

        while "  " in normalized:
            normalized = normalized.replace("  ", " ")

        if self._is_debug and self._input_text != normalized:
            self.logger.debug("\n".join([
                "Text Normalization Complete",
                f"\tInput: {self._input_text}",
                f"\tNormalized: {normalized}"]))

        return normalized
