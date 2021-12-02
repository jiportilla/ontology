#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from nltk.util import ngrams

from base import BaseObject


class NgramGenerator(BaseObject):
    """ Perform n-Gram Generation on Input """

    def __init__(self,
                 some_tokens,
                 ontology_name: str,
                 is_debug: bool = False):
        """
        Created:
            12-Feb-2017
            craig.trim@ibm.com
            *   refactored out of TokenizeText.py
        Updated:
            13-Dec-2019
            craig.trim@ibm.com
            *   pass in ontology name as a param
                https://github.ibm.com/GTS-CDO/unstructured-analytics/pull/1587
        :param some_tokens:
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._tokens = some_tokens
        self._ontology_name = ontology_name

    def perform_semantic_tokenization(self,
                                      tokens):
        from . import PerformSemanticSegmentation

        the_sem_tokens = set()

        for a_token in tokens:
            segmenter = PerformSemanticSegmentation(is_debug=self._is_debug,
                                                    some_input_text=a_token,
                                                    ontology_name=self._ontology_name)
            the_sem_tokens.add(segmenter.process())

        return list(the_sem_tokens)

    def get_ngram_list(self, tokens, level):
        def _list():
            the_list = []
            for item in ngrams(tokens, level):
                the_list.append(" ".join(item).strip())
            return the_list

        return self.perform_semantic_tokenization(_list())

    def process(self):
        result = {
            "gram1": self.get_ngram_list(self._tokens, 1),
            "gram2": self.get_ngram_list(self._tokens, 2),
            "gram3": self.get_ngram_list(self._tokens, 3)}

        return result
