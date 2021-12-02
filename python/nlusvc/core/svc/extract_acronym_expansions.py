#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pandas as pd
import textacy
from pandas import DataFrame

from base import BaseObject


class ExtractAcronymExpansions(BaseObject):
    _d_acronyms = {}
    _d_instances = {}

    def __init__(self,
                 source_text: str,
                 include_acronyms: bool = True,
                 include_instances: bool = True,
                 is_debug: bool = False):
        """
        Created:
            27-Jun-2019
            craig.trim@ibm.com
            *   designed for the purpose of extracting acronyms and abbreviations from text
        """
        BaseObject.__init__(self, __name__)
        from nlusvc.core.dmo import TextExpansionNormalizer
        from nlusvc.core.dto import ExtractAcronymsValidator

        ExtractAcronymsValidator(source_text=source_text,
                                 include_acronyms=include_acronyms,
                                 include_instances=include_instances)

        self.is_debug = is_debug
        self.source_text = source_text
        self.include_acronyms = include_acronyms
        self.include_instances = include_instances

        self.normalizer = TextExpansionNormalizer(is_debug=self.is_debug)

    @staticmethod
    def _preprocess(some_text: str) -> str:
        return textacy.preprocess_text(some_text,
                                       no_urls=True,
                                       no_emails=True,
                                       no_phone_numbers=True,
                                       no_numbers=True,
                                       no_currency_symbols=True,
                                       no_punct=True,
                                       no_contractions=True,
                                       no_accents=True)

    @staticmethod
    def extract(start: int,
                tokens: list) -> (int, list):
        i = start
        candidate = []
        for i in range(start, len(tokens)):
            if ")" in tokens[i]:
                return i, candidate
            candidate.append(tokens[i])
        return i, candidate

    def _extract_acronym(self,
                         start: int,
                         tokens: list,
                         acronym: str) -> None:
        """

        :param start:
            the starting sequence in the entire list of tokens
            e.g.,   7
        :param tokens:
            the entire list of tokens
            e.g.,   [   'the', 'use', 'of',
                        'Natural', 'Language', 'Processing',
                        '(', 'NLP', ')',
                        'is', 'challenging' ]
        :param acronym:
            the candidate acronym
            e.g.,   'NLP'
        :return:
        """

        def _x():
            x = start - len(acronym)
            if x < 0:
                return 0
            return x

        expansion = " ".join(tokens[_x():start])
        expansion = self._preprocess(expansion)

        if acronym not in self._d_acronyms:
            self._d_acronyms[acronym] = set()
        self._d_acronyms[acronym].add(expansion)

    def _extract_instance(self,
                          start: int,
                          tokens: list,
                          candidate: list) -> None:
        """

        :param start:
            the starting sequence in the entire list of tokens
            e.g.,   3
        :param tokens:
            the entire list of tokens
            e.g.,   [   'Deep', 'Learning', '(',
                        'RNNs', ',', 'CNNs', ')' ]
        :param candidate:
            the candidate examples
            e.g.,   [   'RNNs', ',', 'CNNs' ]
        :return:
        """
        instance = " ".join(tokens[:start])
        if "," in instance or "(" in instance:
            return

        if instance not in self._d_instances:
            self._d_instances[instance] = []

        candidate = " ".join(candidate)
        self._d_instances[instance].append(candidate)

    def _process_tokens(self,
                        tokens: list):

        for i in range(0, len(tokens)):
            if tokens[i] != "(":
                continue

            start = i
            i, candidate = self.extract(i + 1, tokens)

            # Extract an Acronym
            if len(candidate) == 1 and self.include_acronyms:
                self._extract_acronym(start=start,
                                      tokens=tokens,
                                      acronym=candidate[0])

            # Extract an Instance
            elif len(candidate) > 1 and self.include_instances:
                self._extract_instance(start=start,
                                       tokens=tokens,
                                       candidate=candidate)

    def _to_result_set(self) -> DataFrame:
        """
        :return:
        """
        results = []

        if self.include_acronyms:
            for key in self._d_acronyms:
                for value in self._d_acronyms[key]:
                    results.append({
                        "Type": "Acronym",
                        "Key": key,
                        "Value": value})

        if self.include_instances:
            for key in self._d_instances:
                for value in self._d_instances[key]:
                    results.append({
                        "Type": "Instance",
                        "Key": key,
                        "Value": value})

        return pd.DataFrame(results)

    def process(self) -> DataFrame:

        for tokens in self.normalizer.process(self.source_text):
            self._process_tokens(tokens)

        return self._to_result_set()
