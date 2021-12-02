#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pprint
from collections import Counter

from base import BaseObject
from datadict import LoadStopWords
from nlusvc import TextAPI


class AddLanguageVariability(BaseObject):
    """ Service to Augment the Synonyms ('Language Variability') file with new entries

        all output from this file represents net-new non-duplicated entries into KB
    """

    _text_api = TextAPI(is_debug=False)
    _stopwords = LoadStopWords(is_debug=True).load(refresh_cache=True)

    def __init__(self,
                 is_debug: bool = True):
        """
        Created:
            25-Jul-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/493
        Updated:
            10-Aug-2019
            craig.trim@ibm.com
            *   completely reritten in pursuit of
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/701
            *   Developer's Note:
                it's likely that we'll end up with multiple recipes for language variability
                each recipe will be a separate domain component and all will be orchestrated via a service
        Updated:
            13-Jan-2020
        """
        BaseObject.__init__(self, __name__)

        self.is_debug = is_debug

    def _count_tokens(self,
                      terms: list) -> Counter:
        """
        Purpose:
            Tokenize input and count the tokens
        :param terms:
            a list of unstructured text
            the list may be of any size
            each item in the list may be of any size
        :return:
            a collection.Counter instance containing a count of tokens in the list
        """
        c = Counter()

        for term in terms:
            [c.update({x: 1}) for x in term.lower().split(' ')
             if x not in self._stopwords]

        return c

    @staticmethod
    def _filter_tokens_by_count(c: Counter,
                                min_threshold: int = 1) -> Counter:
        """
        Purpose:
            filter a collection.Counter instance by count
        :param c:
            a collection.Counter instance
        :param min_threshold:
            the minimium valid count
        :return:
            a new collection.Counter instance
        """
        return Counter({x: c[x] for x in c if c[x] >= min_threshold})

    @staticmethod
    def _subsumed(c: Counter) -> set:
        """
        Purpose:
            Find Subsumed Tokens
        Sample Input:
            [   'redhat_certified_system_administrator',
                'open_stack',
                'system_administrator',
                'redhat' ]
        Sample Output:
            [   'redhat_certified_system_administrator',
                'system_administrator',
                'redhat' ]
            a 'subsumed' token is one that contains another known token as a sub-string
        :param c:
            a collection.Counter instance
        :return:
            a set of subsumed tokens
        """
        subsumed = set()

        for t1 in c:
            for t2 in c:
                if t1 == t2:
                    continue

                if t1 in t2 or t2 in t1:
                    subsumed.add(t1)
                    subsumed.add(t2)

        return subsumed

    @staticmethod
    def _patterns(delta: set,
                  subsumed: set) -> list:
        """
        Purpose:
            Create a list of patterns for token formation
        :param delta:
            a set of tokens that are not subsumed by any other token
            Sample Input:
                {   'open_stack' }
        :param subsumed:
            a set of subsumed tokens (generated from the 'subsumed' function)
            Sample Input:
                {   'redhat_certified_system_administrator',
                    'system_administrator',
                    'redhat' }
        :return:
            a list of candidate patterns
        """
        s = set()
        for t1 in subsumed:
            for t2 in delta:
                if t1 == t2:
                    continue
                s.add(f"{t1}+{t2}")
                s.add(f"{t1}_{t2}")
        return sorted(s)

    def process(self,
                terms: list,
                min_threshold: int = 2) -> list:
        """
        Purpose:
            Given a list of terms, create variations for the synonyms_kb.csv file
        :param terms:
            any list of terms or phrases
        :param min_threshold:
            the minimum token count that is acceptable
            any token beneath this threshold is typically considered low-value
            perhaps useful only for outlier and edge patterns
        :return:
            the variations
        """
        c = self._count_tokens(terms)
        if self.is_debug:
            self.logger.debug('\n'.join([
                "Token Count:",
                pprint.pformat(c.most_common(25))]))

        c = self._filter_tokens_by_count(c, min_threshold)
        if self.is_debug:
            self.logger.debug('\n'.join([
                f"Token Filter (min-threshold={min_threshold}):",
                pprint.pformat(c.most_common(25))]))

        tokens = set([x for x in c])

        subsumed = self._subsumed(c)
        if self.is_debug:
            self.logger.debug(f"Token Subsumption: {subsumed}")

        delta = tokens.difference(subsumed)
        if self.is_debug:
            self.logger.debug(f"Token Delta: {delta}")

        patterns = self._patterns(delta, subsumed)
        if self.is_debug:
            self.logger.debug(f"Pattern Generation: {patterns}")

        return patterns
