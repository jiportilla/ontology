#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import re
from typing import Optional

from base import BaseObject
from base import FileIO


class DBpediaTaxonomyExtractor(BaseObject):
    """ Extract latent 'is-a' hierarchy from unstructured text """

    __isa_patterns = None
    __clause_patterns = None

    def __init__(self,
                 input_text: str,
                 is_debug: bool = False):
        """
        Created:
            7-Jan-2020
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1706
        Updated:
            7-Feb-2020
            craig.trim@ibm.com
            *   moved dictionaries to CSV resources
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1837
        """
        BaseObject.__init__(self, __name__)
        if self.__isa_patterns is None:
            self.__isa_patterns = FileIO.file_to_lines_by_relative_path(
                "resources/config/dbpedia/patterns_isa.csv")
            self.__isa_patterns = [x.lower().strip() for x in self.__isa_patterns]

        if self.__clause_patterns is None:
            self.__clause_patterns = FileIO.file_to_lines_by_relative_path(
                "resources/config/dbpedia/patterns_clause.csv")
            self.__clause_patterns = [x.lower().strip() for x in self.__clause_patterns]

        self._input_text = input_text
        self._is_debug = is_debug

    @staticmethod
    def _remove_parens(input_text: str) -> str:
        """
        Purpose:
            Remove parens
        Sample Input:
            A drug (/drɑːɡ/) is any substance
        Sample Output:
            A drug is any substance
        :return:
            text without parens
        """
        if '(' not in input_text and ')' not in input_text:
            return input_text

        x = input_text.index('(')
        y = input_text.index(')') + 2

        return f"{input_text[0:x]}{input_text[y:]}"

    @staticmethod
    def _remove_akas(input_text: str) -> str:
        """
        Purpose:
            Remove AKA sections
        Sample Input:
            Lung cancer, also known as lung carcinoma, is a malignant lung tumor
        Sample Output:
            Lung cancer is a malignant lung tumor
        :return:
            text without AKA
        """

        patterns = [', also known as ',
                    ', or ',
                    ', formerly known as']

        for pattern in patterns:
            if pattern in input_text:
                x = input_text.index(pattern)
                y = input_text[:(x + len(pattern))].index(',') + x + len(pattern) + 4
                input_text = f"{input_text[:x]}{input_text[y:]}"

        return input_text

    def _cleanse_text(self,
                      input_text: str) -> str:
        original_input_text = input_text
        input_text = self._remove_parens(input_text)
        input_text = self._remove_akas(input_text)

        if self._is_debug and original_input_text != input_text:
            self.logger.debug('\n'.join([
                "Text Cleansing Completed",
                f"\tOriginal: {original_input_text}",
                f"\tNormalized: {input_text}"]))

        return input_text

    def _segmenter(self,
                   input_text: str) -> list:
        from nlutext.core.svc import PerformSentenceSegmentation
        segmenter = PerformSentenceSegmentation(is_debug=self._is_debug)
        return segmenter.process(some_input_text=input_text,
                                 remove_wiki_references=True)

    def _isa_normalizer(self,
                        input_text: str) -> str:

        input_text = input_text.lower().strip()
        for pattern in self.__isa_patterns:
            if pattern in input_text:
                input_text = input_text.replace(pattern, 'is_a')

        return input_text

    def _clause_inducer(self,
                        input_text: str) -> str:

        regex = re.compile(r"[A-Za-z]+\s+(in|of)\s+", re.IGNORECASE)

        target = ', '

        input_text = input_text.lower().strip()
        for candidate in self.__clause_patterns:
            k_mid = f" {candidate} "
            k_start = f"{candidate} "
            k_end = f" {candidate}"

            if input_text.startswith(k_start):
                input_text = input_text.replace(k_start, target)
            elif k_mid in input_text:
                input_text = input_text.replace(k_mid, target)
            elif input_text.endswith(k_end):
                input_text = input_text.replace(k_end, target)

        while True:
            search_result = regex.search(input_text)
            if not search_result:
                break
            input_text = input_text.replace(search_result.group(), target)

        input_text = input_text.strip().replace(f' {target}', target).replace('  ', ' ')

        if input_text.startswith(', '):
            input_text = input_text[2:].strip()

        return input_text

    def process(self) -> Optional[str]:

        if not self._input_text:
            self.logger.warning("SubClass Extraction Failed: No Input")
            return None

        normalized = self._isa_normalizer(self._input_text)
        if 'is_a' not in normalized:
            self.logger.warning('\n'.join([
                "SubClass Extraction Failed: No IS-A",
                f"\tOriginal Text: {self._input_text}",
                f"\tNormalized: {normalized}"]))

            return None

        x = normalized.index('is_a') + len('is_a')
        normalized = normalized[x:].strip()

        normalized = self._clause_inducer(normalized)
        normalized = normalized.replace(',', '.')
        normalized = normalized.replace(';', '.')

        sentences = self._segmenter(normalized)
        subclass = sentences[0].replace('.', '').strip()

        if not subclass:
            self.logger.warning('\n'.join([
                "SubClass Extraction Failed: No SubClass",
                f"\tOriginal Text: {self._input_text}",
                f"\tNormalized: {normalized}",
                f"\tSentences: {sentences}"]))

            return None

        if self._is_debug:
            self.logger.debug('\n'.join([
                "SubClass Extraction Completed",
                f"\tResult: {subclass}",
                f"\tOriginal Text: {self._input_text}",
                f"\tNormalized: {normalized}",
                f"\tSentences: {sentences}"]))

        return subclass
