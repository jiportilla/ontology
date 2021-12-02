#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from base import MandatoryParamError
from datadict import FindEntityNgrams


class PerformSemanticSegmentation(BaseObject):
    """ """

    def __init__(self,
                 some_input_text: str,
                 ontology_name: str = 'base',
                 is_debug: bool = False):
        """
        Updated:
            27-Jul-2017
            craig.trim@ibm.com
            *   replaced 'entities_by_gram_level' with 'FindEntityNgrams'
        Updated:
            21-Feb-2019
            craig.trim@ibm.com
            *   migrated to text
        Updated:
            13-Dec-2019
            craig.trim@ibm.com
            *   load dictionaries by ontology name
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1582
        :param some_input_text:
        """
        BaseObject.__init__(self, __name__)
        if not some_input_text:
            raise MandatoryParamError("Input Text")

        self._is_debug = is_debug
        self._input_text = some_input_text
        self._ontology_name = ontology_name

        self._ngram_finder = FindEntityNgrams(is_debug=is_debug,
                                              ontology_name=ontology_name)

    def segment(self,
                some_input: str) -> str:
        """
        note the use of 'remaining_input' to prevent
        the same string from being segmented

        :param some_input:
        :return: a segmented string
            e.g.
                "please install good work request for me"
            becomes
                "please install good_work_request for me"
        """
        remaining_input = some_input

        n = 4
        while n > 0:
            for gram in self._ngram_finder.by_level(n):
                if gram in remaining_input:
                    some_input = some_input.replace(gram, gram.replace(" ", "_"))
                    remaining_input = remaining_input.replace(gram, "")
            n -= 1

        return some_input

    def process(self) -> str:

        if isinstance(self._input_text, tuple):
            self._input_text = " ".join(self._input_text)

        normalized = self.segment(self._input_text)

        if self._is_debug and self._input_text != normalized:
            self.logger.debug("\n".join([
                "Semantic Segmentation Complete: ",
                f"\tOntology Name: {self._ontology_name}",
                f"\tInput: {self._input_text}",
                f"\tOutput: {normalized}"]))

        return normalized
