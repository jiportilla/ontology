#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import string

from base import BaseObject
from base import MandatoryParamError
from datadict import the_stopwords_dict


class TrigramGenerator(BaseObject):
    """ perform trigram generation on unstructured text """

    def __init__(self,
                 some_injected_stopwords: list,
                 some_gram_length: int = 3,
                 is_debug: bool = False):
        """
        Created:
            7-Mar-2019
            craig.trim@ibm.com
            *   refactored out of unstructured-data-parser
        :param some_injected_stopwords:
            stopwords that are customized to this particular parsing instance
        :param some_gram_length:
            gram length
            e.g.    3 = Trigrams
                    2 = Bigrams
                    1 = Unigrams
        """
        BaseObject.__init__(self, __name__)
        if not some_gram_length:
            raise MandatoryParamError("Gram Length")
        if some_gram_length < 1 or some_gram_length > 4:
            raise MandatoryParamError("Mandatory Gram Length is 1-4")

        self.is_debug = is_debug
        self.gram_length = some_gram_length
        self.stop_words = the_stopwords_dict
        self.injected_stop_words = some_injected_stopwords

    @staticmethod
    def _is_valid(values: list) -> bool:
        """
        determine if a candidate trigram is valid
        :param values:
            three string values to form a candidate trigram
        :return:
            True        candidate trigram is valid
            False       candidate trigram is not valid
        """

        def _has_num() -> bool:
            for v in values:
                if v.startswith("no_"):
                    return True
            return False

        def _has_digit() -> bool:
            for v in values:
                if v.isdigit():
                    return True
            return False

        if _has_digit() or _has_num():
            return False
        return True

    def _tokenize(self,
                  input_text: str) -> list:
        """
        :param input_text:
            an input string of any length
        :return:
            a list of tokens (with invalid tokens redacted)
        """

        def _is_valid(token: str):
            if not token:
                return False
            if len(token) < 3:
                return False
            if len(token) > 25:
                return False
            if token in self.stop_words:
                return False
            if token in self.injected_stop_words:
                return False

            return True

        input_text = input_text.translate((None, string.punctuation))
        return [x.strip().lower() for x in input_text.split(" ") if _is_valid(x)]

    def _quadgrams(self,
                   tokens: list,
                   total_tokens: int) -> list:
        grams = []
        for i in range(0, total_tokens):
            if i + 3 < total_tokens + 1:
                t0 = tokens[i]
                t1 = tokens[i + 1]
                t2 = tokens[i + 2]
                t3 = tokens[i + 3]

                if self._is_valid([t0, t1, t2, t3]):
                    grams.append("{} {} {} {}".format(t0, t1, t2, t3))

        return grams

    def _trigrams(self,
                  tokens: list,
                  total_tokens: int) -> list:
        grams = []
        for i in range(0, total_tokens):
            if i + 2 < total_tokens + 1:
                t0 = tokens[i]
                t1 = tokens[i + 1]
                t2 = tokens[i + 2]

                if self._is_valid([t0, t1, t2]):
                    grams.append("{} {} {}".format(t0, t1, t2))

        return grams

    def _bigrams(self,
                 tokens: list,
                 total_tokens: int) -> list:
        grams = []
        for i in range(0, total_tokens):
            if i + 1 < total_tokens + 1:
                t0 = tokens[i]
                t1 = tokens[i + 1]

                if self._is_valid([t0, t1]):
                    grams.append("{} {}".format(t0, t1))

        return grams

    def _unigrams(self,
                  tokens: list,
                  total_tokens: int) -> list:
        grams = []
        for i in range(0, total_tokens):
            if i < total_tokens + 1:
                t0 = tokens[i]

                if self._is_valid([t0]):
                    grams.append(t0)

        return grams

    def process(self,
                input_text: str) -> list:
        """
        :return:
            a list of trigrams created from the input text
        """

        tokens = self._tokenize(input_text)
        total_tokens = len(tokens) - 1

        def _grams():
            if self.gram_length == 4:
                return self._quadgrams(tokens,
                                       total_tokens)
            if self.gram_length == 3:
                return self._trigrams(tokens,
                                      total_tokens)
            if self.gram_length == 2:
                return self._bigrams(tokens,
                                     total_tokens)
            if self.gram_length == 1:
                return self._unigrams(tokens,
                                      total_tokens)
            raise NotImplementedError("\n".join([
                "Gram Length Not Implemented",
                f"\tlength: {self.gram_length}"]))

        grams = _grams()

        if self.is_debug:
            self.logger.debug("\n".join([
                "Created Input Grams: ",
                f"\tinput-text: {input_text}",
                f"\tlength: {self.gram_length}",
                f"\tInput Trigrams: {grams}"]))

        return grams
