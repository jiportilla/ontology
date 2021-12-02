#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pprint
from typing import Optional

from pandas import DataFrame

from base import BaseObject
from cendalytics.wikipedia.ingest.dmo import DBpediaEntityResolution
from nlusvc import NtlkAPI
from nlusvc import SpacyAPI


class ImplicationFinder(BaseObject):
    """ Find a Partonomy Relationship in Text

    Sample Input (input text):
        Amoebozoa is a major taxonomic group containing about 2,400 described species of
        amoeboid protists, often possessing blunt, fingerlike, lobose pseudopods and
        tubular mitochondrial cristae

    Sample Input (list of links):
        [ ... a list of links from a dbpedia page ... ]

    Intermediate Output (Keywords):
        [   'amoeboid',
            'containing',
            'cristae',
            'described',
            'fingerlike',
            'group',
            'is',
            'lobose',
            'mitochondrial',
            'possessing',
            'protists',
            'pseudopods',
            'species' ]

    Intermediate Output (Matches):
        {   'cristae':          [   'crista',
                                    'cristae'],
            'lobose':           [   'lobosa'],
            'mitochondrial':    [   'mitochondria'],
            'protists':         [   'protist',
                                    'protista'],
            'pseudopods':       [   'pseudopod'] }

    Sample Output:
      {     'Crista':               {'cristae'},
            'Lobosa':               {'lobose'},
            'Mitochondrial DNA':    {'mitochondria', 'mitochondrial'},
            'Potentilla cristae':   {'cristae'},
            'Protist':              {'protists'},
            'Protista taxonomy':    {'protists', 'protista'},
            'Pseudopodia':          {'pseudopod', 'pseudopods'} }

      Each entry in the sample output is a dbPedia entity
      Each set of values for the entry is language variability for that entry
    """

    def __init__(self,
                 input_text: str,
                 candidate_matches: list,
                 is_debug: bool = False):
        """
        Created:
            6-Feb-2020
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1829
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._input_text = input_text
        self._candidate_matches = candidate_matches
        self._spacy_api = SpacyAPI(is_debug=self._is_debug)
        self._nltk_api = NtlkAPI(is_debug=True)

    def _entity_finder(self,
                       some_name: str) -> Optional[str]:
        """
        Purpose:
            Find a dbPedia Entry for a given Term
        Notes:
            This is not an exact science as many terms are highly ambiguous
            in the case of multiple results we take the most likely match
        Sample Input:
            "beta"
        Sample Output:
            "Beta Tech"
        :param some_name:
            any input
        :return:
            a dbpedia entry
        """
        return DBpediaEntityResolution(is_debug=self._is_debug,
                                       some_title=some_name).most_likely_result()

    def _key_words(self):
        df = self._spacy_api.part_of_speech(self._input_text)

        def text_by_pos(a_pos: str) -> list:
            df2 = df[df['POS'] == a_pos]
            return sorted(df2['Text'].unique())

        s = set()
        [s.add(x) for x in text_by_pos('VERB')]
        [s.add(x) for x in text_by_pos('NOUN')]

        return sorted(s)

    def _edit_distance(self,
                       a_keyword: str) -> DataFrame:
        return self._nltk_api.edit_distance_multiple(
            input_text=a_keyword,
            candidate_matches=self._candidate_matches)

    def process(self) -> dict:
        key_words = self._key_words()

        d_matches = {}
        for key_word in key_words:
            df_ed = self._edit_distance(key_word)

            df2 = df_ed[df_ed['Distance'] < 2]
            if df2.empty:
                continue

            matches = sorted(df2['Candidate'].unique())
            if not matches or not len(matches):
                continue

            d_matches[key_word] = matches

        d_resolutions = {}
        for k in d_matches:
            for v in d_matches[k]:
                resolution = self._entity_finder(v)
                if resolution not in d_resolutions:
                    d_resolutions[resolution] = set()
                d_resolutions[resolution].add(v)
                d_resolutions[resolution].add(k)

        # d_master = {}
        # for k in d_resolutions:
        #     values = d_resolutions[k]
        #     values = [value for value in values
        #               if value.lower() != k.lower()]
        #     values = [value for value in values
        #               if '(' not in value]
        #
        #     d_master[k] = values

        if self._is_debug:
            self.logger.debug('\n'.join([
                f"Keywords (total={len(key_words)})",
                f"\t{key_words}",
                f"Matches:",
                pprint.pformat(d_matches),
                f"Resolutions:",
                pprint.pformat(d_resolutions)]))

        return d_resolutions
