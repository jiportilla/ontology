#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from nltk import stem
from nltk import tokenize
from nltk.metrics import edit_distance

from base import BaseObject


class NltkEditDistance(BaseObject):
    """
    An NLTK-based implementation of Edit Distance
    Reference:
        https://streamhacker.com/2011/10/31/fuzzy-string-matching-python/

    Edit Distance is a measure of similarity that uses a confusion matrix
    to determine how likely it is the difference between two terms
    is the result of a keyboard error

    For example, "the" and "teh" have an edit distance of 2
    they are close enough to be considered similar as 'h' and 'e' are
    commonly transposed on a keyboard.

    Edit distance can be confusing if this isn't understood.

    Take these samples:
            s1          s2          d
            Java        Java        0
            Java        Javas       0
            Java        Java2       1
            Java        Java 2      2
            Java        Java VM     3
            Java        Java 2.0    6
            Geology     Fractal     7

    The difference between "Java" and "Java 2" is only 2
    but the difference between "Java" and "Java 2.0" is a 6
    These latter two terms are almost as far apart as "Geology" and "Fractal" are
    And yet they seem very similar.  After all it's just a ".0" addition in the latter case
    which may be easily implied in the former case.

    But keep in mind Edit Distance is not on the basis of semantics or reasoning.
    It's solely formed around a confusion matrix and the mistaken transposition of terms
    A 6 indicates there is almost 0 chance that a user could accidentally type in
    "Java 2.0" and have meant to type in "Java".  But typing "Java2" or "Java 2"
    instead of "Java" is more likely.
    """

    __stemmer = stem.PorterStemmer()

    d_cache_normalize = {}  # cache of normalized terms

    def __init__(self,
                 is_debug:bool=False):
        """
        Created:
            21-Apr-2019
            craig.trim@ibm.com
        Updated:
            6-Feb-2020
            craig.trim@ibm.com
            *   minor updates in pursuit of
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1829
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug

    def _normalize(self,
                   text: str) -> str:
        if text in self.d_cache_normalize:
            return self.d_cache_normalize[text]

        def _process():
            words = tokenize.wordpunct_tokenize(text.lower().strip())
            return ' '.join([self.__stemmer.stem(w) for w in words])

        self.d_cache_normalize[text] = _process()
        return self.d_cache_normalize[text]

    def process(self,
                s1: str,
                s2: str) -> int:
        """
        :param s1:
            an input string
        :param s2:
            a string to compare against
        :return:
            a measure of the distance between the two terms
        """

        return edit_distance(self._normalize(s1),
                             self._normalize(s2))
