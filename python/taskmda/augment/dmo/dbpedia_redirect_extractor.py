#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import time
from typing import Optional

import pandas as pd
from pandas import DataFrame

from base import BaseObject


class DBPediaRedirectExtractor(BaseObject):
    """ extract redirects from an input file """

    def __init__(self,
                 input_file: str,
                 is_debug: bool = False):
        """
        Created:
            9-Jan-2020
            craig.trim@ibm.com
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._input_file = input_file

    @staticmethod
    def _cleanse(a_term: str) -> str:
        return a_term.split('/')[-1].split('>')[0].strip().lower()

    def _load(self) -> DataFrame:
        d_terms = {}

        def _read_file():
            f = open(self._input_file, 'r')
            for some_line in f:
                if some_line.startswith('<'):
                    yield some_line

        for line in _read_file():
            tokens = line.split(' ')
            if len(tokens) != 4:
                print(len(tokens), line)
                raise ValueError

            t1 = self._cleanse(tokens[0])
            t2 = self._cleanse(tokens[2])

            if t2 not in d_terms:
                d_terms[t2] = set()
            d_terms[t2].add(t1)

        results = []
        for k in d_terms:
            for v in d_terms[k]:
                results.append({
                    "Term": k,
                    "Variation": v})

        return pd.DataFrame(results)

    def process(self,
                output_file: Optional[str] = None) -> DataFrame:
        start = time.time()
        df = self._load()
        if output_file:
            df.to_csv(output_file, sep='\t', encoding='utf-8')

        self.logger.debug('\n'.join([
            "Extracted Redirects",
            f"\tSize: {len(df)}",
            f"\tTotal Time: {time.time() - start}"]))

        return df


def main(input_file, output_file):
    # python /Users/craig.trimibm.com/data/workspaces/ibm/text/workspace/taskmda/taskmda/augment/dmo/extract_dbpedia_redirects.py /Users/craig.trimibm.com/Downloads/redirects_en.ttl /Users/craig.trimibm.com/Downloads/redirects.csv
    DBPediaRedirectExtractor(is_debug=True,
                             input_file=input_file).process(output_file)


if __name__ == "__main__":
    import plac

    plac.call(main)
