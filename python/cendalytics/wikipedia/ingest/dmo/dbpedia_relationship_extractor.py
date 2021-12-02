#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import re
from typing import Optional

from base import BaseObject
from base import FileIO


class DBpediaRelationshipExtractor(BaseObject):
    """ Extract latent semantic relationships (other than is-a) from unstructured text """

    __aka_patterns = None
    __partof_patterns = None
    __clause_patterns = None

    def __init__(self,
                 input_text: str,
                 is_debug: bool = False):
        """
        Created:
            22-Jan-2020
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1776
        Updated:
            7-Feb-2020
            craig.trim@ibm.com
            *   moved dictionaries to CSV resources
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1837
        """
        BaseObject.__init__(self, __name__)
        if self.__aka_patterns is None:
            self.__aka_patterns = FileIO.file_to_lines_by_relative_path(
                "resources/config/dbpedia/patterns_aka.csv")

        if self.__partof_patterns is None:
            self.__partof_patterns = FileIO.file_to_lines_by_relative_path(
                "resources/config/dbpedia/patterns_partof.csv")

        if self.__clause_patterns is None:
            self.__clause_patterns = FileIO.file_to_lines_by_relative_path(
                "resources/config/dbpedia/patterns_clause.csv")

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

    def _remove_akas(self,
                     input_text: str) -> str:
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

        for pattern in self.__aka_patterns:
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

    def _partof_normalizer(self,
                           input_text: str) -> str:
        for pattern in self.__partof_patterns:
            if pattern in input_text:
                input_text = input_text.replace(pattern, 'part_of')

        return input_text

    def _clause_inducer(self,
                        input_text: str) -> str:

        regex = re.compile(r"[A-Za-z]+\s+(in|of)\s+", re.IGNORECASE)

        target = ', '
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

    def _extract(self,
                 input_text: str) -> Optional[str]:
        normalized = self._partof_normalizer(input_text)
        if 'part_of' not in normalized:
            return None

        x = normalized.index('part_of') + len('part_of')
        normalized = normalized[x:].strip()

        normalized = self._clause_inducer(normalized)
        normalized = normalized.replace(',', '.')
        normalized = normalized.replace(';', '.')

        sentences = self._segmenter(normalized)
        partonomy = sentences[0].replace('.', '').strip()

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Partonomy Extraction Completed",
                f"\tResult: {partonomy}",
                f"\tOriginal Text: {input_text}",
                f"\tNormalized: {normalized}",
                f"\tSentences: {sentences}"]))

        return partonomy

    def process(self) -> list:
        s = set()
        s.add(self._extract(self._input_text))
        s = [x for x in s if x]

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Partonomy Extraction Completed",
                f"\tResults: {s}",
                f"\tInput: {self._input_text}"]))

        return sorted(s)
