#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from enum import Enum

import spacy
import textacy.extract
import textacy.keyterms

from base import BaseObject


class KeyTerm(Enum):
    TEXTRANK = 0  # fast, unigrams
    SGRANK = 1  # very time intensive, phrase matching
    SINGLERANK = 2  # fast-ish, long phrase based output


class TextacyKeytermRanking(BaseObject):
    """ wrapper around textacy functionality
    """

    _nlp = None

    def __init__(self):
        """
        Created:
            3-Apr-2019
            craig.trim@ibm.com
        Updated:
            11-Apr-2019
            craig.trim@ibm.com
            *   renamed from textacy-keyterm-identifier
        """
        BaseObject.__init__(self, __name__)

    @staticmethod
    def identify_key_terms(doc: textacy.doc,
                           algorithm: str,
                           number_of_terms: int,
                           ngrams=(1, 2, 3, 4)) -> list:

        if algorithm == "SGRANK":
            return textacy.keyterms.sgrank(doc,
                                           ngrams=ngrams,
                                           normalize='lower',
                                           n_keyterms=(number_of_terms / 100))
        if algorithm == "TEXTRANK":
            return textacy.keyterms.textrank(doc,
                                             normalize='lemma',
                                             n_keyterms=number_of_terms)
        if algorithm == "SINGLERANK":
            return textacy.keyterms.singlerank(doc,
                                               normalize='lemma',
                                               n_keyterms=number_of_terms)
        else:
            raise NotImplementedError("\n".join([
                "Key Terms Algorithm Not Implemented",
                "\tvalue: {}".format(algorithm)
            ]))

    def _to_doc(self,
                some_text: str) -> textacy.doc:
        if not self._nlp:
            self._nlp = spacy.load("en_core_web_sm")
        return textacy.make_spacy_doc(self._nlp(some_text))

    def by_list(self,
                some_list: list,
                algorithm: KeyTerm,
                number_of_terms=10) -> list:

        return self.identify_key_terms(doc=self._to_doc(" ".join(some_list)),
                                       algorithm=algorithm.name,
                                       number_of_terms=number_of_terms,
                                       ngrams=(2, 3, 4))

    def by_text(self,
                some_text: str,
                algorithm: KeyTerm,
                number_of_terms=10) -> list:
        return self.identify_key_terms(doc=self._to_doc(some_text),
                                       algorithm=algorithm.name,
                                       number_of_terms=number_of_terms)

    def by_doc(self,
               some_doc: textacy.doc,
               algorithm: KeyTerm,
               number_of_terms=10) -> list:
        return self.identify_key_terms(doc=some_doc,
                                       algorithm=algorithm.name,
                                       number_of_terms=number_of_terms)
