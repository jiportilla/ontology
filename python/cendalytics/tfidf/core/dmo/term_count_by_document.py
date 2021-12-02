#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject


class TermCountByDocument(BaseObject):
    """
    Purpose:
        Count the terms in each document
    Notes:
        this effectively gives us the "size" of a document
    Sample Output:
        {   '000009659': 2,
            '000015659': 3,
            '000062661': 3,
            '000072806': 1,
            '000076661': 2,
            '0000QH744': 73,
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

    def process(self):
        """
        :return:
            a dictionary keyed by documents with the value being
                a count of terms in that document
        """
        d = {}
        for document in self._d_terms_by_document:
            total = 0
            for term in self._d_terms_by_document[document]:
                total += self._d_terms_by_document[document][term]
            d[document] = total
        return d
