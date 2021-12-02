#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pprint
from math import log

from base import BaseObject
from base import MandatoryParamError
from datadict import FindEntity
from nlutag.core.dto.token_match import TokenMatches


class ExactEntityMatcher(BaseObject):

    def __init__(self,
                 some_input: dict,
                 some_matches: TokenMatches,
                 ontology_name: str,
                 is_debug=False):
        """
        Updated:
            2-Nov-2016
            craig.trim@ibm.com
            *   vastly simplified - removed spanning logic, no longer using skipgrams
                also differentiate n-gram vs skip-gram confidence via 10% heuristic
        Updated:
            8-Dec-2016
            craig.trim@ibm.com
            *   revmap returns a list rather than a string
        Updated:
            12-Apr-2017
            craig.trim@ibm.com
            *   renamed from "ExactProductMatcher"
        Updated:
            21-Jun-2017
            craig.trim@ibm.com
            *   control logging via debug flag
        Updated:
            21-Feb-2019
            craig.trim@ibm.com
            *   migrated to text
        Updated:
            26-Mar-2019
            craig.trim@ibm.com
            *   updates made based on MDA changes
        Updated:
            26-Aug-2019
            craig.trim@ibm.com
            *   updated scoring methods
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/796
        Updated:
            5-Sept-2019
            craig.trim@ibm.com
            *   fixed a scoring defect
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/872
        Updated:
            13-Dec-2019
            craig.trim@ibm.com
            *   add ontology name
                https://github.ibm.com/GTS-CDO/unstructured-analytics/pull/1587
        :param some_input:
        :param some_matches:
        """
        BaseObject.__init__(self, __name__)
        if not some_input:
            raise MandatoryParamError("Input")
        if not some_matches:
            raise MandatoryParamError("Matches")

        self._is_debug = is_debug
        self._d_input = some_input
        self._matches = some_matches
        self._ontology_name = ontology_name

        self._entity_finder = FindEntity(is_debug=self._is_debug,
                                         ontology_name=self._ontology_name)

        if self._is_debug:
            self.logger.debug("\n".join([
                "Initialized Exact Entity Matcher",
                f"\tOntology Name: {self._ontology_name}",
                f"\tInput: {pprint.pformat(self._d_input, indent=4)}",
                f"\tMatches: {pprint.pformat(self._matches.get_matches(), indent=4)}"]))

    @staticmethod
    def get_variants(some_set) -> set:
        """
        Purpose:
            29-May-2017
            craig.trim@ibm.com
            *   we need both forms of the token with "_" and without to create match
        :param some_set:
        :return:
        """
        s = set()

        for value in some_set:
            s.add(value.replace("_", " "))

        return s

    def get_input_ngrams(self) -> set:
        """
        :return: all the possible n-gram configurations
            of the original user input

            these are precomputed in the textpreprocessor package
        """
        the_ngrams = set()

        the_ngrams = the_ngrams.union(
            set(self._d_input["tokens"]["ngrams"]["gram1"]))
        the_ngrams = the_ngrams.union(
            set(self._d_input["tokens"]["ngrams"]["gram2"]))
        the_ngrams = the_ngrams.union(
            set(self._d_input["tokens"]["ngrams"]["gram3"]))
        the_ngrams = the_ngrams.union(self.get_variants(the_ngrams))

        if self._is_debug:
            self.logger.debug("\n".join([
                "n-Grams",
                pprint.pformat(the_ngrams, indent=4)]))

        return the_ngrams

    def get_input_skipgrams(self) -> set:
        """
        :return: all the possible skip-gram configurations
            of the original user input

            these are precomputed in the textpreprocessor package
        """
        the_ngrams = set()

        def _has_skipgrams():
            if "skipgrams" not in self._d_input["tokens"]:
                return False
            if len(self._d_input["tokens"]["skipgrams"]) == 0:
                return False
            return True

        if _has_skipgrams():

            the_ngrams = the_ngrams.union(
                set(self._d_input["tokens"]["skipgrams"]["n2k2"]))
            the_ngrams = the_ngrams.union(
                set(self._d_input["tokens"]["skipgrams"]["n3k2"]))
            the_ngrams = the_ngrams.union(
                set(self._d_input["tokens"]["skipgrams"]["n3k3"]))
            the_ngrams = the_ngrams.union(
                set(self._d_input["tokens"]["skipgrams"]["n4k3"]))
            the_ngrams = the_ngrams.union(self.get_variants(the_ngrams))

            if self._is_debug:
                self.logger.debug("\n".join([
                    "Skip Grams",
                    pprint.pformat(the_ngrams, indent=4)]))

        return the_ngrams

    @staticmethod
    def _total_input_ngrams(input_ngrams):
        if len(input_ngrams) > 0:
            return round(log(len(input_ngrams), 10), 1)
        return 1

    def process(self):

        d_parents_revmap = self._entity_finder.d_parents_revmap()

        input_ngrams = self.get_input_ngrams()
        total_input_ngrams = self._total_input_ngrams(input_ngrams)
        total_tokens = len(self._d_input['normalized'].split(' '))

        for token in input_ngrams:

            label = self._entity_finder.label(token)
            if not label:
                continue

            label_size = len(label.split(' '))
            score = round(100 - (total_tokens / label_size) - total_input_ngrams, 1)
            if score < 0:
                score = 0

            self._matches.add(some_key=label,
                              some_match=token,
                              some_type="exact",
                              some_sub_type="n-gram",
                              some_confidence=score)

            if self._is_debug:
                self.logger.debug(f"Score Calculated "
                                  f"(type=exact-1,"
                                  f" label=({label}, {label_size}),"
                                  f" factors (ngrams={total_input_ngrams},"
                                  f" tokens={total_tokens})")

        for token in self.get_input_skipgrams():
            if token in d_parents_revmap:
                for some_token in d_parents_revmap[token]:

                    some_token_size = len(some_token.split(' '))
                    score = round(75 - (total_tokens / some_token_size) - total_input_ngrams, 1)
                    if score < 0:
                        score = 0

                    self._matches.add(some_key=some_token,
                                      some_match=token,
                                      some_type="exact",
                                      some_sub_type="skip-gram",
                                      some_confidence=score)

                    if self._is_debug:
                        self.logger.debug(f"Score Calculated "
                                          f"(type=exact-2,"
                                          f" label=({some_token}, {some_token_size}),"
                                          f" factors (ngrams={total_input_ngrams},"
                                          f" tokens={total_tokens})")

        if self._is_debug:
            if len(self._matches.get_matches()) > 0:
                self.logger.debug("\n".join([
                    "Found Direct Matches",
                    pprint.pformat(self._matches.get_matches(), indent=4)]))

            else:
                self.logger.debug("No Direct Matches Found")
