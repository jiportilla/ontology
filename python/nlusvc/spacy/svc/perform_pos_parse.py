#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pandas as pd
from pandas import DataFrame
from spacy.lang.en import English
from tabulate import tabulate

from base import BaseObject


class PerformPosParse(BaseObject):
    """ Perform POS (Part-of-Speech) Parse with spaCy

    Sample Input:
        Amoebozoa is a major taxonomic group containing about 2,400 described species of
        amoeboid protists, often possessing blunt, fingerlike, lobose pseudopods and
        tubular mitochondrial cristae.

    Sample Output:
        +----+----------+-----------+----------+---------------+-------+---------+-------+---------------+
        |    | Dep      | IsAlpha   | IsStop   | Lemma         | POS   | Shape   | Tag   | Text          |
        |----+----------+-----------+----------+---------------+-------+---------+-------+---------------|
        |  0 | nsubj    | True      | False    | amoebozoa     | PROPN | Xxxxx   | NNP   | Amoebozoa     |
        |  1 | ROOT     | True      | True     | be            | VERB  | xx      | VBZ   | is            |
        |  2 | det      | True      | True     | a             | DET   | x       | DT    | a             |
        |  3 | amod     | True      | False    | major         | ADJ   | xxxx    | JJ    | major         |
        |  4 | amod     | True      | False    | taxonomic     | ADJ   | xxxx    | JJ    | taxonomic     |
        |  5 | attr     | True      | False    | group         | NOUN  | xxxx    | NN    | group         |
        |  6 | acl      | True      | False    | contain       | VERB  | xxxx    | VBG   | containing    |
        |  7 | quantmod | True      | True     | about         | ADV   | xxxx    | RB    | about         |
        |  8 | nummod   | False     | False    | 2,400         | NUM   | d,ddd   | CD    | 2,400         |
        |  9 | amod     | True      | False    | describe      | VERB  | xxxx    | VBN   | described     |
        | 10 | dobj     | True      | False    | specie        | NOUN  | xxxx    | NNS   | species       |
        | 11 | prep     | True      | True     | of            | ADP   | xx      | IN    | of            |
        | 12 | compound | True      | False    | amoeboid      | NOUN  | xxxx    | NN    | amoeboid      |
        | 13 | pobj     | True      | False    | protist       | NOUN  | xxxx    | NNS   | protists      |
        | 14 | punct    | False     | False    | ,             | PUNCT | ,       | ,     | ,             |
        | 15 | advmod   | True      | True     | often         | ADV   | xxxx    | RB    | often         |
        | 16 | acl      | True      | False    | possess       | VERB  | xxxx    | VBG   | possessing    |
        | 17 | dobj     | True      | False    | blunt         | ADJ   | xxxx    | JJ    | blunt         |
        | 18 | punct    | False     | False    | ,             | PUNCT | ,       | ,     | ,             |
        | 19 | conj     | True      | False    | fingerlike    | NOUN  | xxxx    | NN    | fingerlike    |
        | 20 | punct    | False     | False    | ,             | PUNCT | ,       | ,     | ,             |
        | 21 | amod     | True      | False    | lobose        | VERB  | xxxx    | VB    | lobose        |
        | 22 | conj     | True      | False    | pseudopod     | NOUN  | xxxx    | NNS   | pseudopods    |
        | 23 | cc       | True      | True     | and           | CCONJ | xxx     | CC    | and           |
        | 24 | amod     | True      | False    | tubular       | ADJ   | xxxx    | JJ    | tubular       |
        | 25 | amod     | True      | False    | mitochondrial | NOUN  | xxxx    | NN    | mitochondrial |
        | 26 | conj     | True      | False    | cristae       | VERB  | xxxx    | VBN   | cristae       |
        | 27 | punct    | False     | False    | .             | PUNCT | .       | .     | .             |
        +----+----------+-----------+----------+---------------+-------+---------+-------+---------------+

    Reference:
        https://spacy.io/usage/linguistic-features

    """

    def __init__(self,
                 nlp: English,
                 input_text: str,
                 is_debug: bool = False):
        """
        Created:
            6-Feb-2020
            craig.trim@ibm.com
            *   in pursuit of
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1829
        """
        BaseObject.__init__(self, __name__)

        self._nlp = nlp
        self._is_debug = is_debug
        self._input_text = input_text

    def process(self,
                log_sample_size: int = 500) -> DataFrame:
        """
        Purpose:
            Perform spaCY pos-tagging on input text
        :param log_sample_size:
        :return:
            a dataframe with the following columns:
                Text: The original word text.
                Lemma: The base form of the word.
                POS: The simple part-of-speech tag.
                Tag: The detailed part-of-speech tag.
                Dep: Syntactic dependency, i.e. the relation between tokens.
                Shape: The word shape – capitalization, punctuation, digits.
                IsAlpha: Is the token an alpha character?
                IsStop: Is the token part of a stop list, i.e. the most common words of the language?
        """
        doc = self._nlp(self._input_text)

        results = []
        for token in doc:
            results.append({
                "Text": token.text,
                "Lemma": token.lemma_,
                "POS": token.pos_,
                "Tag": token.tag_,
                "Dep": token.dep_,
                "Shape": token.shape_,
                "IsAlpha": token.is_alpha,
                "IsStop": token.is_stop})

        df = pd.DataFrame(results)
        if self._is_debug:
            self.logger.debug('\n'.join([
                "Part-of-Speech DataFrame Generated",
                f"\tSize: {len(df)}",
                tabulate(df.head(log_sample_size),
                         tablefmt='psql',
                         headers='keys')]))

        return df
