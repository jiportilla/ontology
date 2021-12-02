#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject


class TextExpansionNormalizer(BaseObject):

    def __init__(self,
                 is_debug: bool = False):
        """
        Created:
            27-Jun-2019
            craig.trim@ibm.com
            *   designed for the purpose of normalizing text (pre-processing)
                prior to extracting acronyms and abbreviations
        """
        BaseObject.__init__(self, __name__)
        self.is_debug = is_debug

    @staticmethod
    def _normalize(a_line: str) -> str:
        a_line = a_line.replace("(", "( ")
        a_line = a_line.replace(")", " )")
        a_line = a_line.replace(",", " , ")

        while "  " in a_line:
            a_line = a_line.replace("  ", " ")

        return a_line

    def process(self,
                some_text: str) -> list:
        lines = [line.strip() for line in some_text.split("\n")]
        lines = [self._normalize(line) for line in lines]
        lines = [[x.strip() for x in line.split(" ") if x]
                 for line in lines if line]

        return [x for x in lines if len(x)]
