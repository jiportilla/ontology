#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pandas as pd
import spacy
from pandas import DataFrame
from tabulate import tabulate

from base import BaseObject
from base import DataTypeError


class PerformPosTagging(BaseObject):
    """
    Perform Part-of-Speech (POS) tagging

    based on spaCy
    https://spacy.io/usage/linguistic-features
    """

    _nlp = spacy.load('en')

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
    def _upper_pos(part_of_speech: str) -> str:
        part_of_speech = part_of_speech.upper()

        adjectives = ["ADJ"]
        nouns = ["PROPN", "NOUN"]
        phrases = ["ADP", "CCONJ"]
        verbs = ["VERB"]

        if part_of_speech in adjectives:
            return "ADJ"
        if part_of_speech in nouns:
            return "NOUN"
        if part_of_speech in phrases:
            return "CONNECT"
        if part_of_speech in verbs:
            return "VERB"
        return "OTHER"

    @staticmethod
    def _normalize_pos(lemma: str,
                       part_of_speech: str):
        """
        Purpose:
            we almost always accept spaCy's decision about part-of-speech tagging
            this function will encode all exceptions
        :param lemma:
            the text being tagged
        :param part_of_speech:
            spaCy part-of-speech
        :return:
            a normalized part-of-speech
        """
        if lemma == "®":
            return "OTHER"

        return part_of_speech

    def _tagger(self,
                input_text: str) -> DataFrame:
        input_text = input_text
        doc = self._nlp(input_text)

        results = []
        for token in doc:
            _normalized_pos = self._normalize_pos(token.lemma_,
                                                  token.pos_)
            _upper_pos = self._upper_pos(_normalized_pos)

            results.append({
                "Lemma": token.lemma_,
                "PartOfSpeech": _normalized_pos,
                "PartOfSpeechMeta": _upper_pos,
                "Tag": token.tag_,
                "Dependency": token.dep_,
                "Shape": token.shape_,
                "IsAlpha": token.is_alpha,
                "Stopword": token.is_stop})

        df = pd.DataFrame(results)
        if self.is_debug:
            self.logger.debug("\n".join([
                "Part-of-Speech Output",
                tabulate(df,
                         headers='keys',
                         tablefmt='psql')]))

        return df

    def _from_str(self,
                  input_text: str) -> DataFrame:
        if type(input_text) != str:
            raise DataTypeError("\n".join([
                "Invalid DataType"]))

        return self._tagger(input_text)

    def _from_list(self,
                   multiple_inputs: list):
        if type(multiple_inputs) != list:
            raise DataTypeError("\n".join([
                "Invalid DataType"]))

        results = []
        [results.append(self._tagger(x)) for x in multiple_inputs]
        return results

    def process(self,
                some_input: str or list):
        def inner():
            if type(some_input) == str:
                return self._from_str(some_input)
            if type(some_input) == list:
                return self._from_list(some_input)

            self.logger.error('\n'.join([
                "Unrecognized Input",
                f"\tInput: {some_input}",
                f"\tType: {type(some_input)}"]))
            raise DataTypeError

        results = inner()

        return results
