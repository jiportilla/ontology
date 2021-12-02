#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from base import BaseObject


class SkipgramGenerator(BaseObject):
    def __init__(self, some_tokens):
        """
        Created:
            12-Feb-2017
            craig.trim@ibm.com
            *   refactored out of TokenizeText.py
        Updated:
            21-Feb-2017
            craig.trim@ibm.com
            *   is this module still necessary?
                - does long-distance tag matching evolve away from skip-gram usage?
                - where are skip-grams actually used?
        Updated:
            3-Apr-2017
            craig.trim@ibm.com
            *   don't log full results (they are VERY large)
        :param some_tokens:
        """
        BaseObject.__init__(self, __name__)
        self.tokens = some_tokens

    @staticmethod
    def perform_semantic_tokenization(tokens):
        from nlutext import PerformSemanticSegmentation

        the_sem_tokens = set()

        for a_token in tokens:
            the_sem_tokens.add(PerformSemanticSegmentation(a_token).process())

        return list(the_sem_tokens)

    def compute_skipgrams(self, tokens, n, k):
        from nlutext import ComputeSkipGrams

        try:
            return self.perform_semantic_tokenization(
                ComputeSkipGrams().process(
                    tokens, n, k))
        except:
            return []

    def process(self):
        result = {
            "n2k2": self.compute_skipgrams(self.tokens, 2, 2),
            "n3k2": self.compute_skipgrams(self.tokens, 3, 2),
            "n3k3": self.compute_skipgrams(self.tokens, 3, 3),
            "n4k3": self.compute_skipgrams(self.tokens, 4, 3)}

        return result
