#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import math

import pandas as pd
from pandas import DataFrame

from base import BaseObject


class TfIdfComputer(BaseObject):
    """
    Purpose:
        Compute the TF-IDF Score
    """

    def __init__(self,
                 d_terms_by_document: dict,
                 d_documents_by_term: dict,
                 term_count_by_document: dict,
                 term_count_in_corpus: dict,
                 is_debug: bool = False):
        """
        Created:
            5-Nov-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1261#issuecomment-15732844
        :param d_terms_by_document:
            a dictionary keyed by documents with the value
                a dictionary of term/freqency pairs for that document
            Sample Input:
                {   '000009659': {  'agile': 1, 'ibm': 1},
                    '000015659': {  'design thinking': 1, 'enterprise': 1, 'role': 1},
                    '000062661': {  'design thinking': 1, 'enterprise': 1, 'role': 1},
                    '000072806': {  'cognitive skill': 1},
                    '000076661': {  'ibm': 1, 'mentor': 1},
                    ...
                }
        :param d_documents_by_term:
            a dictionary of terms with the value
                a list of documents containing that term
            Sample Input:
                {   '4g network': ['09223D744', '03095U744', ... '084661661'],
                    '5g network': ['03095U744'],
                    'access': ['06256H744', '096729744', ... '5G5626897'],
                    ...
                }
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._d_terms_by_document = d_terms_by_document
        self._d_documents_by_term = d_documents_by_term
        self._total_documents = len(self._d_terms_by_document)
        self._term_count_in_corpus = term_count_in_corpus
        self._term_count_by_document = term_count_by_document

    def _count_term_frequency_in_document(self,
                                          a_document: str,
                                          a_term: str) -> int:
        """
        Purpose:
            Number of times term t appears in a document
        :param a_term:
        :return:
        """
        return self._d_terms_by_document[a_document][a_term]

    def _count_total_terms_in_document(self,
                                       a_document: str) -> int:
        """
        Purpose:
            Total number of terms in the document
        :param a_document:
        :return:
        """
        return self._term_count_by_document[a_document]

    def _term_frequency(self,
                        a_term: str,
                        a_document: str) -> float:
        """
        Definition:
            TF: Term Frequency, which measures how frequently a term occurs in a document.
            Since every document is different in length, it is possible
                that a term would appear much more times in long documents than shorter ones.
            Thus, the term frequency is often divided by the document length
                (aka. the total number of terms in the document) as a way of normalization:
            TF(t) = (Number of times term t appears in a document) / (Total number of terms in the document).
        Reference:
            http://www.tfidf.com/
        :return:
        """
        x = self._count_term_frequency_in_document(a_term=a_term,
                                                   a_document=a_document)
        y = self._count_total_terms_in_document(a_document)
        return x / y

    def _count_documents_with_term(self,
                                   a_term: str) -> int:
        """
        Purpose:
            Count the number of documents with term t in it
        :param a_term:
            any given term
        :return:
            a count of documents with this term
        """
        return len(self._d_documents_by_term[a_term])

    def _inverse_document_frequency(self,
                                    a_term: str):
        """
        Definition:
            IDF: Inverse Document Frequency, which measures how important a term is.
            While computing TF, all terms are considered equally important.
            However it is known that certain terms, such as "is", "of", and "that",
                may appear a lot of times but have little importance.
            Thus we need to weigh down the frequent terms while scale up the rare ones,
                by computing the following:
            IDF(t) = log_e(Total number of documents / Number of documents with term t in it).
        Reference:
            http://www.tfidf.com/
        :return:
        """
        x = self._total_documents
        y = self._count_documents_with_term(a_term)
        return math.log(x / y)

    def process(self,
                rounding_threshold: int = 5) -> DataFrame:

        results = []
        for document in self._d_terms_by_document:
            for term in self._d_terms_by_document[document]:
                tf = self._term_frequency(a_term=term,
                                          a_document=document)
                idf = self._inverse_document_frequency(a_term=term)

                def _tfidf() -> float or None:
                    if idf == 0:
                        self.logger.warning(f"IDF is 0 ("
                                            f"total-documents={self._total_documents}, "
                                            f"docs-with-term={self._count_documents_with_term(term)})")
                        return None
                    return tf / idf

                tfidf = _tfidf()
                if not tfidf:
                    continue

                results.append({
                    "Doc": document,
                    "Term": term,
                    "TF": round(tf, rounding_threshold),
                    "IDF": round(idf, rounding_threshold),
                    "TFIDF": round(tfidf, rounding_threshold),
                    "TotalDocs": self._total_documents,
                    "TermsInDoc": self._count_total_terms_in_document(document),
                    "TermFrequencyInDoc": self._count_term_frequency_in_document(a_document=document,
                                                                                 a_term=term),
                    "TermFrequencyInCorpus": self._term_count_in_corpus[term],
                    "DocsWithTerm": self._count_documents_with_term(term)})

        return pd.DataFrame(results)
