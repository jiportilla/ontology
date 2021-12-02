#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import textacy

from base import BaseObject
from datadict import LoadStopWords


class CasedTermExtraction(BaseObject):
    """
    Purpose:
    Extract Cased Terms

    sample input:
        One of these services is Amazon Elastic Compute Cloud, which allows users to
        have at their disposal a virtual cluster of computers, available all the time,
        through the Internet

    sample output:
        Amazon Elastic Compute Cloud """

    _stopwords = LoadStopWords().load()

    def __init__(self):
        """
        Updated:
            10-Jul-2019
            craig.trim@ibm.com
            *   refactored out of 'dbpedia-term-extractor'
        """
        BaseObject.__init__(self, __name__)

    @staticmethod
    def from_list(a_list: list) -> list:
        s = set()

        extractor = CasedTermExtraction()

        def update(x):
            [s.add(result) for result in extractor._process(x)]

        [update(x) for x in a_list]

        return sorted(s)

    @staticmethod
    def from_str(a_str: str) -> list:
        return CasedTermExtraction()._process(a_str)

    def _cleanse(self,
                 some_patterns: list) -> list:
        s = set()

        for a_pattern in some_patterns:
            a_pattern = a_pattern.replace("'s", "")
            a_pattern = a_pattern.replace("&", "")
            a_pattern = textacy.preprocess_text(a_pattern,
                                                no_punct=True)

            tokens = a_pattern.split(" ")
            first_token = tokens[0].lower()
            if first_token in self._stopwords:
                a_pattern = " ".join(tokens[1:])

            s.add(a_pattern)

        return sorted([x for x in s if " " in x])

    @staticmethod
    def _subsumes(some_patterns: list) -> list:
        s = set(some_patterns)
        eliminate = set()

        for p1 in some_patterns:
            for p2 in some_patterns:
                if p1 == p2:
                    continue
                if p1 in p2:
                    eliminate.add(p1)
                elif p2 in p1:
                    eliminate.add(p2)

        return sorted(s.difference(eliminate))

    def _process(self,
                 some_input: str) -> list:
        results = []
        tokens = some_input.split(" ")

        def _is_valid(a_token: str):
            return a_token[0:1].isupper() and \
                   a_token.isalpha() and \
                   len(a_token) > 1

        def _iterate(ctr: int):
            result = []
            while _is_valid(tokens[ctr]):
                result.append(tokens[ctr])
                if tokens[ctr].endswith("."):
                    return result, ctr
                elif ctr + 1 >= len(tokens):
                    return result, ctr
                ctr += 1

            return result, ctr

        for i in range(0, len(tokens)):
            if len(tokens[i]) < 2:
                continue

            pattern, i = _iterate(i)
            if len(pattern) > 1:
                results.append(" ".join(pattern))

        return self._cleanse(self._subsumes(results))
