#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from pandas import DataFrame

from base import BaseObject


class NtlkAPI(BaseObject):
    """ An NLTK API """

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
        self._is_debug = is_debug

    def edit_distance(self,
                      input_text: str,
                      candidate_match: str) -> int:
        from nlusvc.nltk.svc import FindEditDistance

        svc = FindEditDistance(is_debug=self._is_debug)
        return svc.single(input_text=input_text,
                          candidate_match=candidate_match)

    def edit_distance_multiple(self,
                               input_text: str,
                               candidate_matches: list) -> DataFrame:
        """
        Purpose:
            Provide NLTK edit distance matching
        :param input_text:
            a string value to compare to
            Sample Input:
                cristae
        :param candidate_matches:
            a list of one-or-more key terms
            Sample Input:
                'Crista',
                ...
                'Cutaneous amoebiasis'
        :return:
            a pandas DataFrame
            Sample Output:
            +----+----------------------+------------+---------+
            |    | Candidate            |   Distance | Input   |
            |----+----------------------+------------+---------|
            |  4 | crista               |          0 | cristae |
            ...
            | 10 | cutaneous amoebiasis |         13 | cristae |
            +----+----------------------+------------+---------+
        """
        from nlusvc.nltk.svc import FindEditDistance

        svc = FindEditDistance(is_debug=self._is_debug)
        return svc.multiple(input_text=input_text,
                            candidate_matches=candidate_matches)


if __name__ == "__main__":
    tokens = ['Conosa',
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
              'Cyanobacteria']
    df = NtlkAPI(is_debug=True).edit_distance_multiple(input_text="cristae", candidate_matches=tokens)
    from tabulate import tabulate
    print(tabulate(df, tablefmt='psql', headers='keys'))
