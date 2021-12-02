#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pprint

from base import BaseObject


class NormalizeIncomingText(BaseObject):

    def __init__(self,
                 ontology_name: str = 'base',
                 is_debug: bool = False):
        """
        Updated:
            12-Feb-2017
            craig.trim@ibm.com
            *   replaced "perform_synonym_swapping" with refactored class call:
                PerformSynonymSwapping(normalized, <iterations>).process()
            *   renamed to 'normalize_incoming_text.py'
            *   removed "dmo/text_normalizer.py" and replaced wtih
                get_normalized_text() function
            *   refactored text tokenization into separate service
        Updated:
            14-Mar-2017
            craig.trim@ibm.com
            *   added 'expand-apostrophes'
        Updated:
            21-Feb-2019
            craig.trim@ibm.com
            *   migrate to text
        Updated:
            7-Mar-2019
            craig.trim@ibm.com
            *   add validity checks to process method
        Updated:
            11-April-2019
            anassar@us.ibm.com
            *   change to use synonym swap v2
        Updated:
            23-Apr-2019
            craig.trim@ibm.com
            *   add caching and move text param to 'process' method
        Updated:
            29-May-2019
            craig.trim@ibm.com
            *   remove naive caching and experiment with Redis
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/316
                (removed on 5-Jun-2019)
        Updated:
            28-Oct-2019
            craig.trim@ibm.com
            *   moved 'preprocess' method over from text-parser
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1206
        Updated:
            22-Nov-2019
            craig.trim@ibm.com
            *   decide not to comment out a certain line
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1419#issuecomment-16190214
        Updated:
            13-Dec-2019
            craig.trim@ibm.com
            *   load dictionaries by ontology name
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1582
        """
        BaseObject.__init__(self, __name__)

        self._is_debug = is_debug
        self._ontology_name = ontology_name

    @staticmethod
    def remove_spaces(normalized: str) -> str:
        while "  " in normalized:
            normalized = normalized.replace("  ", " ")

        return normalized

    @staticmethod
    def _is_valid(x: str) -> bool:
        if not x:
            return False
        if not len(x):
            return False
        return True

    def _normalize(self,
                   value: str) -> dict:
        from nlutext.core.dmo import ApostropheExpansion
        from nlutext.core.dmo import EnclicticExpansion
        from nlutext.core.dmo import PunctuationRemover
        from nlutext.core.dmo import SynonymSwapper
        from nlutext.core.dmo import TextNormalizer
        from nlutext.core.dmo import TextPreprocessor

        normalized = TextPreprocessor(input_text=value,
                                      is_debug=self._is_debug).process()

        if self._is_valid(normalized):
            normalized = self.remove_spaces(normalized)
            normalized = TextNormalizer(some_input_text=normalized,
                                        is_debug=self._is_debug).process()

        if self._is_valid(normalized):
            normalized = EnclicticExpansion(the_input_text=normalized,
                                            is_debug=self._is_debug).process()

        if self._is_valid(normalized):
            normalized = ApostropheExpansion(the_input_text=normalized,
                                             is_debug=self._is_debug).process()

        if self._is_valid(normalized):
            normalized = PunctuationRemover(the_input_text=normalized,
                                            is_debug=self._is_debug).process()

        if self._is_valid(normalized):
            normalized = SynonymSwapper(some_iterations=3,
                                        some_input=normalized,
                                        ontology_name=self._ontology_name,  # GIT-1582-16611092
                                        is_debug=self._is_debug).process()
            normalized = self.remove_spaces(normalized)

        # commented out in # GIT-1419-16190214
        # common normalization pattern that adds no value
        # normalized = normalized.replace("exclamation", "")

        svcresult = {
            "original": value,
            "normalized": normalized}

        if self._is_debug:
            self.logger.debug("\n".join([
                "Text Normalization Complete",
                pprint.pformat(svcresult, indent=4)]))

        return svcresult

    def process(self,
                value: str) -> dict:

        result = self._normalize(value)

        return result
