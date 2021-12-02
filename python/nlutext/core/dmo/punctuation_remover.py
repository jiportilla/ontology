#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import re

from base import BaseObject
from base import MandatoryParamError

PUNCTUATION = ["!", '"', "$", "%", "&", "\\", "'", "(", ")", "*",
               "+", ",", "/", ";", "<", "=", ".",
               ">", "?", "@", "[", "]", "^", "`", "{", "|", "}", "~", u"…"]

PUNCTUATION_REGEXP = re.compile(u"|".join([u"\\" + special_char for special_char in PUNCTUATION]))
BLANKS_REGEXP = re.compile(r"\s+")


class PunctuationRemover(BaseObject):

    def __init__(self,
                 the_input_text: str,
                 is_debug: bool = False):
        """
        Updated:
            2-Aug-2016
            craig.trim@ibm.com
            *   broke py_web
                translate() takes exactly one argument (2 given)
        Updated:
            9-Aug-2016
            craig.trim@ibm.com
            *   put back in, broke unit tests to take out
                normalized = self.input.translate(string.maketrans("",""), string.punctuation)
        Updated:
            14-Nov-2016
            craig.trim@ibm.com
            *   removed hyphens from punctuation array
        Updated:
            24-Feb-2017
            craig.trim@ibm.com
            *   removed periods from punctuation array
                may need to redesign this based around #977
            *   reverted this change - broke some tests
        Updated:
            17-Mar-2017
            craig.trim@ibm.com
            *   removed hyphen from punctuation array
        Updated:
            19-Apr-2017
            craig.trim@ibm.com
            *   renamed from "RemoveExtraneousPunctuation"
        Updated:
            21-Feb-2019
            craig.trim@ibm.com
            *   migrated to text
        Updated:
            14-Dec-2019
            craig.trim@ibm.com
            *   change punctuation replacement strategy
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1613#issuecomment-16624776
        :param the_input_text:
        """
        BaseObject.__init__(self, __name__)
        if not the_input_text:
            raise MandatoryParamError("Input Text")

        self.is_debug = is_debug
        self.input = the_input_text

    @staticmethod
    def strip_punctuation(some_text):
        stripped = PUNCTUATION_REGEXP.sub(' ', some_text)  # GIT-1613-16624776
        stripped = BLANKS_REGEXP.sub(' ', stripped)

        while '  ' in stripped:
            stripped = stripped.replace('  ', ' ')

        return stripped

    def process(self):
        normalized = self.strip_punctuation(self.input)

        if self.is_debug and self.input != normalized:
            self.logger.debug("\n".join([
                "Extraneous Punctuation Removed:",
                f"\tInput: {self.input}",
                f"\tOutput: {normalized}"]))

        return normalized
