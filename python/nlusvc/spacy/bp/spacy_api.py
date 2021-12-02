#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import spacy
from pandas import DataFrame

from base import BaseObject


class SpacyAPI(BaseObject):
    """ API around common spaCy functions """

    __nlp = None

    def __init__(self,
                 is_debug: bool = False):
        """
        Created:
            6-Feb-2020
            craig.trim@ibm.com
            *   in pursuit of
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1829
        """
        BaseObject.__init__(self, __name__)
        if self.__nlp is None:
            self.__nlp = spacy.load("en_core_web_sm")
        self._is_debug = is_debug

    def part_of_speech(self,
                       input_text: str) -> DataFrame:
        """
        Purpose:
            Use spaCy parser for part-of-speech annotation
        Sample Input:
            Amoebozoa is a ... cristae.
        Sample Output:
            +----+----------+-----------+----------+---------------+-------+---------+-------+---------------+
            |    | Dep      | IsAlpha   | IsStop   | Lemma         | POS   | Shape   | Tag   | Text          |
            |----+----------+-----------+----------+---------------+-------+---------+-------+---------------|
            |  0 | nsubj    | True      | False    | amoebozoa     | PROPN | Xxxxx   | NNP   | Amoebozoa     |
            |  1 | ROOT     | True      | True     | be            | VERB  | xx      | VBZ   | is            |
            |  2 | det      | True      | True     | a             | DET   | x       | DT    | a             |
            ...
            | 26 | conj     | True      | False    | cristae       | VERB  | xxxx    | VBN   | cristae       |
            | 27 | punct    | False     | False    | .             | PUNCT | .       | .     | .             |
            +----+----------+-----------+----------+---------------+-------+---------+-------+---------------+

        :param input_text:
        :return:
            a pandas DataFrame of the results
        """
        from nlusvc.spacy.svc import PerformPosParse

        return PerformPosParse(nlp=self.__nlp,
                               input_text=input_text,
                               is_debug=self._is_debug).process()
