#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pprint

from nltk import stem

from base import BaseObject
from nlusvc.core.dmo import FuzzyWuzzyMatcher
from nlusvc.nltk.dmo import NltkEditDistance


class CendantFuzzyMatcher(BaseObject):
    """
    """

    stemmer = stem.PorterStemmer()

    def __init__(self):
        """
        Created:
            21-Apr-2019
            craig.trim@ibm.com
        """
        BaseObject.__init__(self, __name__)
        self._edit_distance = NltkEditDistance()
        self._fuzzy_wuzzy = FuzzyWuzzyMatcher()

    def fuzzywuzzy(self,
                   s1: str,
                   s2: str,
                   is_debug=True) -> dict:
        result = self._fuzzy_wuzzy.process(s1, s2,
                                           basic=True,
                                           q_ratio=False,
                                           w_ratio=True,
                                           uq_ratio=False,
                                           uw_ratio=False,
                                           partial_ratio=True,
                                           token_sort_ratio=False)
        if is_debug:
            self.logger.debug("\n".join([
                "\n{}".format(pprint.pformat(result, indent=4))]))

        return result

    def edit_distance(self,
                      s1: str,
                      s2: str,
                      is_debug=True) -> int:
        """
        :param s1:
            an input string
        :param s2:
            a string to compare against
        :param is_debug:
        :return:
            the edit distance
            the lower the value, the closer the two strings match
        """

        distance = self._edit_distance.process(s1=s1,
                                               s2=s2)
        if is_debug:
            self.logger.debug("\n".join([
                "Edit Distance (s1={}, s2={}, d={})".format(
                    s1, s2, distance)]))
        return distance


if __name__ == "__main__":
    cfm = CendantFuzzyMatcher()
    cfm.edit_distance("the", "teh")
    cfm.edit_distance("Java", "Java VM")
    cfm.edit_distance("Geology", "Fractal")

    cfm.fuzzywuzzy("the", "teh")
    cfm.fuzzywuzzy("Java", "Java VM")
    cfm.fuzzywuzzy("Java", "Java2")
    cfm.fuzzywuzzy("Java", "Java 2")
    cfm.fuzzywuzzy("Java", "Java 2.0")
    cfm.fuzzywuzzy("Geology", "Fractal")
