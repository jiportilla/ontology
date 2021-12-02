#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject


class DocIndexByTerm(BaseObject):
    """
    Purpose:
        Build a Reverse Index keyed by Term
        with a list of documents for each term
    Sample Input:
        {   '4g network': ['09223D744', '03095U744', '412976744', '084661661'],
            '5g network': ['03095U744'],
            'access': ['06256H744', '096729744', '02380N744', ... '5G5626897'],
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
                a list of documents containing that term
        """
        revmap = {}

        for document in self._d_terms_by_document:
            for term in self._d_terms_by_document[document]:
                if term not in revmap:
                    revmap[term] = set()
                revmap[term].add(document)

        return revmap
