#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pandas as pd
from pandas import DataFrame

from base import BaseObject


class FindEditDistance(BaseObject):
    """ Perform Edit Distance Matching across multiple strings """

    def __init__(self,
                 is_debug: bool = False):
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

    def multiple(self,
                 input_text: str,
                 candidate_matches: list) -> DataFrame:
        """
        Sample Input:
            Input Text:
                cristae
            Candidate Matches:
                'Conosa',
                'Corallochytrea',
                'Corbihelia',
                'Crenarchaeota',
                'Crista',
                'Cristae',
                'Cristidiscoidea',
                'Cryptista',
                'Cryptophyta',
                'Ctenophora',
                'Cutaneous amoebiasis',
                'Cutosa',
                'Cutosea',
                'Cyanobacteria'
        Sample Output:
            +----+----------------------+------------+---------+
            |    | Candidate            |   Distance | Input   |
            |----+----------------------+------------+---------|
            |  4 | crista               |          0 | cristae |
            |  5 | cristae              |          0 | cristae |
            |  7 | cryptista            |          3 | cristae |
            |  0 | conosa               |          4 | cristae |
            | 11 | cutosa               |          4 | cristae |
            | 12 | cutosea              |          4 | cristae |
            |  2 | corbihelia           |          6 | cristae |
            |  8 | cryptophyta          |          7 | cristae |
            |  9 | ctenophora           |          8 | cristae |
            |  3 | crenarchaeota        |          9 | cristae |
            |  6 | cristidiscoidea      |          9 | cristae |
            |  1 | corallochytrea       |         10 | cristae |
            | 13 | cyanobacteria        |         10 | cristae |
            | 10 | cutaneous amoebiasis |         13 | cristae |
            +----+----------------------+------------+---------+
        :param input_text:
            an input string
        :param candidate_matches:
            a list of one-or-more matches to compare against
        :return:
            a measure of the distance between the two terms
        """
        results = []

        input_text = input_text.lower().strip()
        candidate_matches = [x.lower().strip() for x in candidate_matches]

        for candidate_match in candidate_matches:
            result = self.single(input_text=input_text,
                                 candidate_match=candidate_match)
            results.append({
                "Input": input_text,
                "Candidate": candidate_match,
                "Distance": result})

        df = pd.DataFrame(results)
        return df.sort_values(by=['Distance'], ascending=True)

    def single(self,
               input_text: str,
               candidate_match: str) -> int:
        """
        :param input_text:
            an input string
        :param candidate_match:
            a single match to compare against
        :return:
            a measure of the distance between the two terms
        """
        from nlusvc.nltk.dmo import NltkEditDistance

        dmo = NltkEditDistance(is_debug=self._is_debug)
        return dmo.process(s1=input_text,
                           s2=candidate_match)
