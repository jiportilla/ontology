#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import string

import pandas as pd
from pandas import DataFrame

from base import BaseObject


class TextQueryWindower(BaseObject):
    """ Window Text Query Results
    """

    __exclude = set(string.punctuation)

    def __init__(self,
                 query_results: dict,
                 is_debug: bool = False):
        """
        Created:
            craig.trim@ibm.com
            16-Oct-2019
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1122#issuecomment-15340437
        :param text_parser_results
            the text parser results
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)

        self._is_debug = is_debug
        self._query_results = query_results

    def _to_text(self):
        """
        Purpose:
            Transform Query results into pure text
        :return:
            return a list of text results only
        """
        values = set()
        for cnum in self._query_results:
            [values.add(d['value']) for d in self._query_results[cnum]]

        return sorted(values)

    def _tokens(self,
                term: str,
                input_text: str) -> list:
        input_text = input_text.lower().replace('\t', ' ')
        input_text = ''.join(ch for ch in input_text if ch not in self.__exclude)

        tokens = input_text.split(' ')
        tokens = [x.strip() for x in tokens if x and len(x.strip())]
        tokens = [x.lower() for x in tokens]

        if ' ' not in term:  # return unigrams
            return tokens

        if term.count(' ') == 1:  # return bigrams
            s = set()
            for i in range(0, len(tokens)):
                if i + 1 < len(tokens):
                    s.add(f"{tokens[i]} {tokens[i + 1]}")

            return sorted(s)

        raise NotImplementedError

    def process(self,
                term: str,
                window_size: int = 5) -> DataFrame:
        """

        :param term:
        :param window_size:
        :return:
        """
        master = []
        term = term.lower().strip()

        for input_text in self._to_text():

            tokens = self._tokens(term, input_text)
            n = tokens.index(term)

            def pos_x():
                if n - window_size >= 0:
                    return n - window_size
                return 0

            def pos_y():
                if n + window_size < len(tokens):
                    return n + window_size
                return len(tokens)

            x = pos_x()
            y = pos_y()

            def l_context():
                return ' '.join(tokens[x:n]).strip()

            def r_context():
                return ' '.join(tokens[n + 1:y]).strip()

            master.append({
                "A": l_context(),
                "B": tokens[n],
                "C": r_context()})

        return pd.DataFrame(master).sort_values(
            by=['A'], ascending=False)
