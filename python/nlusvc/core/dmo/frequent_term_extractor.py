#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pprint
from collections import Counter

from base import BaseObject
from base import StringIO
from datadict import LoadStopWords
from datadict import LoadWordnet
from nlutext import NormalizeIncomingText


class FrequentTermExtractor(BaseObject):
    """
    Create frqeuency based lists of terms

    Sample Input:
        (view output from 'freeform-text-query)

    Sample Output:
        {   'mainframe': Counter({  'mw': 13,
                                    'ibms': 4,
                                    'middleware': 3,
                                    ...
                                    'racf': 1,
                                    'ssl': 1,
                                    'virtual_machine': 1}),
            'oracle': Counter({     'peoplesoft': 4,
                                    'architect': 1,
                                    'billable': 1,
                                    ...
                                    'imi': 1,
                                    'mvs': 1,
                                    'sql': 1}),
            ...
        }
    """

    def __init__(self):
        """
        Created:
            23-Mar-2019
            craig.trim@ibm.com
        """
        BaseObject.__init__(self, __name__)

        self.d_cache = {}
        self.stopwords = self._stopwords()

    def _stopwords(self):
        stopwords = sorted(set(LoadStopWords().load() + LoadWordnet().load()))

        self.logger.debug("\n".join([
            "Loaded Stopwords (including WordNet)",
            "\ttotal: {}".format(len(stopwords))
        ]))

        return stopwords

    def _tokens(self,
                d_terms: dict) -> list:
        tokens = []
        [[tokens.append(value) for value in values]
         for values in d_terms.values()]

        self.logger.debug("\n".join([
            "Total Tokens (of all kinds)",
            "\ttotal: {}".format(len(d_terms))
        ]))

        return tokens

    def _words(self,
               tokens: list) -> Counter:
        """
        :param tokens:
            a list of tokens (of any kind)
        :return:
            a list of ascii-word frequencies
        """

        words = StringIO.ascii(tokens)
        self.logger.debug("\n".join([
            "Total Words (ascii only)",
            "\ttotal: {}".format(len(words))
        ]))

        return words

    def _remove_stopwords(self,
                          words: Counter) -> Counter:
        """
        :param words:
            a Counter of ascii-words and their frequencies
        :return:
            a Counter but with all the stopwords removed
        """
        diff = sorted(set(words).difference(self.stopwords))
        diff_count = Counter()

        [diff_count.update({x.strip(): words[x]}) for x in diff]
        self.logger.debug("\n".join([
            "Located Total Words (total = {})".format(len(diff)),
            pprint.pformat(diff_count.most_common(5))
        ]))

        return diff_count

    def _normalize(self,
                   words: Counter) -> Counter:
        """
        :param words:
            a Counter of ascii-words and their frequencies
        :return:
            the normalized form of the incoming token
        """
        c = Counter()

        def _normalizer(a_token: str) -> str:
            return NormalizeIncomingText().process(a_token)["normalized"].strip()

        def _cache(a_token: str) -> str:
            if a_token in self.d_cache:
                return self.d_cache[a_token]
            self.d_cache[a_token] = _normalizer(a_token)
            return self.d_cache[a_token]

        [c.update({_cache(x[0]): x[1]}) for x in words.most_common(1000)]
        c.most_common(5)

        return c

    def process(self,
                d_terms: dict) -> Counter:
        # Step 1: Load all terms from incoming dictionaries
        tokens = self._tokens(d_terms)

        # Step 2: Filter the terms to ascii words only
        words = self._words(tokens)

        # Step 3: Remove stopwords from the prior set
        words = self._remove_stopwords(words)

        # Step 4: Normalize the remaining set
        words = self._normalize(words)

        self.logger.debug("\n".join([
            "Frequent Term Extraction Completed",
            "\ttotal: {}".format(len(words))
        ]))

        return words
