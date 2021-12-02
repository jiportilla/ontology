#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from pandas import DataFrame

from base import BaseObject


class GenerateNounVariations(BaseObject):
    """
    Given a Part-of-Speech (POS) tagged DataFrame, generate Noun variations
    """

    def __init__(self,

                 is_debug: bool = False):
        """
        Created:
            9-Jul-2019
            craig.trim@ibm.com
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)
        self.is_debug = is_debug

    @staticmethod
    def _extract_adj_nouns(df_part_of_speech: DataFrame) -> dict:
        """
        Purpose:
            Find and Return ADJ/NOUN combinations
        Sample Input:
            The (OTHER) Quick (ADJ) Brown (ADJ) Fox (NOUN)
        Sample Output:
            Quick Fox, Brown Fox, Quick Brown Fox, Brown Quick Fox
        :param df_part_of_speech:
            a part-of-speech tagged DataFrame
        :return:
            generated variations
        """
        adjs = []
        nouns = []
        d_terms = {}

        def _generate():
            if len(adjs) and len(nouns):

                _noun = " ".join(nouns)
                _adj = " ".join(adjs)

                key = "{} {}".format(_adj, _noun)
                if key not in d_terms:
                    d_terms[key] = set()

                d_terms[key].add(_noun)
                d_terms[key].add("{} {}".format(" ".join(reversed(adjs)), _noun))

                for adj in adjs:
                    d_terms[key].add("{} {}".format(adj, _noun))

        for _, row in df_part_of_speech.iterrows():
            meta_tag = row["PartOfSpeechMeta"]

            if meta_tag == "ADJ":
                adjs.append(row["Lemma"])
            elif meta_tag == "NOUN":
                nouns.append(row["Lemma"])
            else:
                _generate()
                adjs = []
                nouns = []

        _generate()
        return d_terms

    @staticmethod
    def _extract_verb_nouns(df: DataFrame) -> dict:

        d_terms = {}

        metas = list(df['PartOfSpeechMeta'])
        if "VERB CONNECT NOUN" not in " ".join(metas):
            return d_terms

        lemmas = list(df['Lemma'])

        x = metas.index("VERB")
        if metas[x + 1] == "CONNECT" and metas[x + 2] == "NOUN":

            key = "{} {} {}".format(lemmas[x],
                                    lemmas[x + 1],
                                    lemmas[x + 2])

            if key not in d_terms:
                d_terms[key] = set()
            d_terms[key].add("{} {}".format(lemmas[x + 2], lemmas[x]))
            d_terms[key].add("{} {}".format(lemmas[x], lemmas[x + 2]))

        return d_terms

    @staticmethod
    def _seq(a: list,
             n: int = 3) -> list:
        """
        Extract Contiguous Numerical Sequences from a List
        Sample Input:
            [1, 2, 4, 7, 8, 12, 13, 14, 16, 17, 19]
        Sample Output (n=2):
            [[12, 13], [13, 14]]
        Sample Output (n=3):
            [[12, 13, 14]]
        :param a: 
            the input list of integers
        :param n: 
            the contiguous sequence size
        :return: 
            the list of sequences
        """
        if n == 1:
            return [[x] for x in a]

        seqs = []
        for i in range(0, len(a)):
            if i + n >= len(a):
                continue

            def _match():
                s = set()
                for j in range(0, n):
                    if a[i + j] + 1 == a[i + j + 1]:
                        s.add(a[i + j])
                        s.add(a[i + j + 1])
                    else:
                        s = set()

                if len(s) == n:
                    return sorted(s)

            m = _match()
            if m:
                seqs.append(m)

        return seqs

    def _extract_noun_sequences(self,
                                df: DataFrame,
                                n: int) -> set:
        lemmas = list(df['Lemma'])
        metas = list(df['PartOfSpeechMeta'])

        indices = [i for i, x in enumerate(metas) if x == "NOUN"]
        sequences = self._seq(indices, n=n)

        terms = set()
        for seq in sequences:
            terms.add(" ".join(lemmas[seq[0]:seq[len(seq) - 1] + 1]))

        return terms

    def process(self,
                df_part_of_speech: DataFrame) -> dict:
        return {
            "NN2": self._extract_noun_sequences(df_part_of_speech, n=2),
            "NN3": self._extract_noun_sequences(df_part_of_speech, n=3),
            "NN4": self._extract_noun_sequences(df_part_of_speech, n=4),
            "ADJNN": self._extract_adj_nouns(df_part_of_speech),
            "VCN": self._extract_verb_nouns(df_part_of_speech)}
