#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import string

from spacy.tokens import Doc

from base import BaseObject
from base import MandatoryParamError


class PerformDeepTokenization(BaseObject):
    """ """

    def __init__(self,
                 doc: Doc,
                 ontology_name: str,
                 is_debug: bool = False):
        """
        Created:
            19-Apr-2017
            craig.trim@ibm.com
            *   refactored out of 'PerformSimpleTokenization'
        Updated:
            6-Mar-2019
            craig.trim@ibm.com
            *   make every step optional
        Updated:
            21-Aug-2019
            craig.trim@ibm.com
            *   use spaCy
        Updated:
            14-Nov-2019
            craig.trim@ibm.com
            *   remove unused code in pursuit of
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1365#issue-10831576
        Updated:
            13-Dec-2019
            craig.trim@ibm.com
            *   pass in ontology name as a param
                https://github.ibm.com/GTS-CDO/unstructured-analytics/pull/1587
        :param doc:
            a spaCy doc
        """
        BaseObject.__init__(self, __name__)
        if not doc:
            raise MandatoryParamError("Tokens")

        self._doc = doc
        self._is_debug = is_debug
        self._ontology_name = ontology_name

    @staticmethod
    def _is_valid(term: str,
                  stopwords: list,
                  min_threshold=3) -> bool:
        """ perform stopword filtering """
        if len(term) < min_threshold:
            return False
        if term in stopwords:
            return False

        alpha = string.ascii_lowercase
        for x in term:
            if x not in alpha:
                return False

        return True

    def _remove_stopwords(self,
                          tokens: list,
                          stopwords: list):
        """ remove stopwords from tokens """
        tokens = [x.lower().strip() for x in tokens if x]  # lower case all
        stopwords = [x.lower().strip() for x in stopwords if x]  # lower case all
        return [x for x in tokens if self._is_valid(x, stopwords)]

    def process(self,
                ngram_generation=True) -> dict:

        tokens = []
        for token in self._doc:
            tokens.append(token.text)

        tokens = [x.replace('\n', '').strip() for x in tokens if x]
        tokens = [x for x in tokens if x and len(x)]

        def _ngrams():
            if ngram_generation:
                from nlutext.core.dmo import NgramGenerator

                return NgramGenerator(some_tokens=tokens,
                                      is_debug=self._is_debug,
                                      ontology_name=self._ontology_name).process()
            return []

        ngrams = _ngrams()

        svcresult = {
            "normalized": tokens,
            "ngrams": ngrams}

        return svcresult
