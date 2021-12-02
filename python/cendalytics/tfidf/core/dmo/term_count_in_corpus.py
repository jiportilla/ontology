#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from collections import Counter

from base import BaseObject


class TermCountInCorpus(BaseObject):
    """
    Purpose:
        Generate a Term Count across the Corpus

    Sample Output:
        {   '4g network': 4,
            '5g network': 1,
            'access': 378,
            'access control': 45,
            'access manager': 17,
            'accessibility': 10,
            ...
        }
    """

    def __init__(self,
                 d_terms_by_document: dict,
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
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._d_terms_by_document = d_terms_by_document

    def process(self) -> dict:
        """
        :return:
            a dictionary keyed by terms with the value being
                the number of times that term occurs in the corpus
        """
        c = Counter()
        for document in self._d_terms_by_document:
            for term in self._d_terms_by_document[document]:
                c.update({term: self._d_terms_by_document[document][term]})

        return dict(c)
