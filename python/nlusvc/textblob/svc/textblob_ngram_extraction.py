#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from collections import Counter

from textblob import TextBlob

from base import BaseObject
from datadict import FindStopword
from nlusvc import TextAPI


class TextblobNgramExtraction(BaseObject):
    """
    https://textblob.readthedocs.io/en/dev/quickstart.html
    """

    def __init__(self,
                 is_debug=True):
        """
        Created:
            23-Oct-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1186
        """
        BaseObject.__init__(self, __name__)

        self._is_debug = is_debug
        self._text_api = TextAPI(is_debug=is_debug)

    @staticmethod
    def _ngrams(input_text: str,
                n: int) -> list:
        blob = TextBlob(input_text.lower().strip())
        return [list(x) for x in blob.ngrams(n)]

    @staticmethod
    def _remove_all_stopwords(ngrams: list) -> list:
        def has_stopword(an_ngram: list) -> bool:
            return sum([FindStopword(x).exists() for x in an_ngram]) > 0

        cleansed = []
        [cleansed.append(x) for x in ngrams if not has_stopword(x)]

        return cleansed

    @staticmethod
    def _remove_partial_stopwords(ngrams: list) -> list:
        """
        Purpose:
            Detect First and Last nGram for Stopwords
        Rationale:
            Valid Trigrams (and higher) may have a stopword in between
            e.g.,   "Certification in RedHat"
        :param ngrams:
        :return:
        """

        def has_stopword(an_ngram: list) -> bool:
            if FindStopword(an_ngram[0]).exists():
                return True
            if len(an_ngram) > 1 and FindStopword(an_ngram[len(an_ngram) - 1]).exists():
                return True

        cleansed = []
        [cleansed.append(x) for x in ngrams if not has_stopword(x)]

        return cleansed

    @staticmethod
    def _to_string(ngrams: list) -> list:
        return [' '.join(x) for x in ngrams]

    def process(self,
                lines: list,
                n: int = 2,
                normalize_text: bool = False,
                remove_all_stopwords: bool = True,
                remove_partial_stopwords: bool = False) -> Counter:
        c = Counter()

        for line in lines:
            if normalize_text:
                line = self._text_api.normalize(line)

            ngrams = self._ngrams(line, n=n)
            if remove_all_stopwords:
                ngrams = self._remove_all_stopwords(ngrams)
            if remove_partial_stopwords:
                ngrams = self._remove_partial_stopwords(ngrams)
            [c.update({x: 1}) for x in self._to_string(ngrams)]

        return c


if __name__ == "__main__":
    some_lines = ["AUTO-TRANSLATION OF SOURCE STRINGS IN GLOBAL VERIFICATION TESTING IN A FUNCTIONAL TESTING TOOL"]
    print(TextblobNgramExtraction(is_debug=True).process(lines=some_lines, n=2, normalize_text=True))
