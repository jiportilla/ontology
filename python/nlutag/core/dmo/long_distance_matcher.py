#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pprint
import re
from math import log

from nltk.stem.snowball import SnowballStemmer
from spacy.tokens import Doc

from base import BaseObject
from datadict import FindPatterns
from nlutag.core.dto.token_match import TokenMatches

LDN_FALSE_POSITIVES = {
    "shar": ["sharepoint"]
}


class LongDistanceMatcher(BaseObject):
    _regexps = {}
    _stems = {}

    def __init__(self,
                 doc: Doc,
                 some_input: dict,
                 some_matches: TokenMatches,
                 ontology_name: str,
                 is_debug: bool = False):
        """
        Updated:
            31-Oct-2016
            craig.trim@ibm.com
            *   added 'modify_input_sentence' function
            *   added regexp to 'has_startswith_values'
            *   added '|e|ed|ing|es|eth|er|esses|ly' after running regression-test-for-tags
        Updated:
            8-Dec-2016
            craig.trim@ibm.com
            *   revmap returns a list rather than a string
        Updated:
            12-Feb-2017
            craig.trim@ibm.com
            *   added unicode support throughout
        Updated:
            13-Feb-2017
            craig.trim@ibm.com
            *   ran an informal test with the penalty removed
                this did not seem to impact the tag regression test
                Refactor Request: conduct a formal test, and if impact <= minimal
                completely remove this complex logic
        Updated:
            21-Feb-2017
            craig.trim@ibm.com
            *   replaced this line
                    penalty = FormationPenaltyComputer().process(key, the_input_ngrams)
                with this line
                    penalty = 0
                this does not appear to have impacted the regression test suite for tags
        Updated:
            21-Feb-2019
            craig.trim@ibm.com
            *   migrated to text
        Updated:
            26-Mar-2019
            craig.trim@ibm.com
            *   updates made based on MDA changes
            *   start using 'find-patterns'
            *   add more strict typing
        Updated:
            5-Aug-2019
            craig.trim@ibm.com
            *   updated 'get-token-regexp' method
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/607
        Updated:
            21-Aug-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/796
                -   integrate with spaCy doc
                -   add scoring function
                -   added 'total_input_ngrams / 2' to other ldm matches
                    Rationale:  the more input ngrams exist in the input
                                the less precise ldm (long distance matching) will be
                                hence while this deduction is heuristic
        Updated:
            28-Oct-2019
            craig.trim@ibm.com
            *   add score-normalization routine in pursuit of
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1202
        Updated:
            13-Dec-2019
            craig.trim@ibm.com
            *   pass in ontology name as a param
                https://github.ibm.com/GTS-CDO/unstructured-analytics/pull/1587
        :param doc:
            a spaCy document representing the (normalized) text
        :param some_input:
            an input data structure
        :param some_matches:
            candidate token matches data structure
        """
        BaseObject.__init__(self, __name__)
        self._doc = doc
        self._d_input = some_input
        self._matches = some_matches
        self._ontology_name = ontology_name

        self._is_debug = is_debug
        self._pattern_finder = FindPatterns(is_debug=self._is_debug,
                                            ontology_name=self._ontology_name)
        self._stemmer = SnowballStemmer(language="english",
                                        ignore_stopwords=False)

    @staticmethod
    def normalize(some_dict):
        the_normalized_dict = {}

        for key in some_dict:
            a_normalized_key = key.replace("_", " ")
            the_normalized_dict[a_normalized_key] = some_dict[key]

        return the_normalized_dict

    @staticmethod
    def has_exact_values(some_list: list,
                         the_input_ngrams: list):
        for a_list_token in some_list:
            if a_list_token not in the_input_ngrams:
                return False

        return True

    @staticmethod
    def modify_input_sentence(some_sentence, a_tuple_token):
        """
        Purpose:
            some stemmed tokens lead to false positive matches
            e.g. "shar" needs to match on
                [sharing, shared, shares, share, etc]
            but not on
                [sharepoint]
            this function will modify the search string to remove false positives
        :param some_sentence:
        :param a_tuple_token:
        :return:
            modified sentence
        """
        for key, values in LDN_FALSE_POSITIVES.items():
            if key != a_tuple_token:
                for value in values:
                    some_sentence = some_sentence.replace(value, "")

        return some_sentence

    def _get_token_regexp(self, token):
        """
        Purpose:
            Precise Matching with Regular Expressions

            use a regular expression to avoid a token like
                "ie"
            from matching inside a word like
                "tried"

            or to prevent a sequence such as
                "system recovery"
            from matching
                "system r"

        Reference:
            https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/607#issuecomment-13672801
        :param token:
            any sequence of unstructured text
        :return:
            a compiled regular expression
        """
        if token not in self._regexps:
            expr = "(?:^|\\s){0}(?:$|\\s|(e |e$)|(ed |ed$)|(ing |ing$)|(es |es$)|(eth |eth$)|(er |er$)|(esses |esses$)|(ly |ly$))"
            self._regexps[token] = re.compile(expr.format(token))

        return self._regexps[token]

    def has_startswith_values(self,
                              some_list: list,
                              the_input_ngrams: list) -> bool:
        """
        Purpose:
            prevent matching on incorrect values
            the token "miss" is found in the tokens
                [mission, permission, missing, misses, missed, etc]
            but the first two permutations are not valid


        Created:
        :param some_list:
        :param the_input_ngrams:
        :return:
            True        a match is detected
            False       no match detected
        """
        sent = " ".join(the_input_ngrams)
        for a_tuple_token in some_list:

            normalized_sent = self.modify_input_sentence(sent, a_tuple_token)

            regexp = self._get_token_regexp(a_tuple_token.strip())
            matches = regexp.search(normalized_sent)
            if not matches:
                return False

        return True

    def _get_token_stem(self, token):
        if token not in self._stems:
            self._stems[token] = self._stemmer.stem(token)
        return self._stems[token]

    def stem(self,
             the_tokens: list) -> list:
        the_stemmed_tokens = []

        for a_token in the_tokens:
            the_stemmed_tokens.append(self._get_token_stem(a_token))

        return list(set(the_stemmed_tokens))

    def _score(self,
               pattern: list) -> float:
        """
        Purpose:
            Provide a confidence level score for a long-distance match
        Reference:
            https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/796
        :param pattern:
            the candidate match pattern
        :return:
            a confidence level score
        """
        from . import LongDistanceScoring

        d_pos = {}
        for item in pattern:
            if item not in d_pos:
                d_pos[item] = []

            for token in self._doc:
                if token.text == item:
                    d_pos[item].append(token.i)

        return LongDistanceScoring(d_input=d_pos,
                                   is_debug=self._is_debug).score()

    @staticmethod
    def _normalize_score(score: float) -> float:
        score = round(score, 1)
        if score < 0.0:
            return 0.01
        if score >= 100.0:
            return 100.0
        return score

    def process(self):

        the_input_ngrams = self._d_input["tokens"]["normalized"]
        total_input_ngrams = round(log(len(the_input_ngrams), 10), 1)
        total_tokens = len(self._d_input['normalized'].split(' '))

        stemmed_ngrams = self.stem(the_input_ngrams)

        # for key in the_entity_revmap:
        long_distance_patterns = self._pattern_finder.long_distance()

        for formations in long_distance_patterns:

            for formation in formations:
                penalty = 0

                label = formation["label"]
                pattern = formation["pattern"]
                label_size = len(label.split(' '))

                # try the normal tuple
                if self.has_exact_values(pattern, the_input_ngrams):

                    score = self._normalize_score(self._score(pattern) + penalty)
                    self._matches.add(some_key=label,
                                      some_match=pattern,
                                      some_type="ldm",
                                      some_sub_type="n-tuple",
                                      some_confidence=score)

                # try a stemmed tuple
                elif self.has_startswith_values(pattern,
                                                stemmed_ngrams):
                    score = self._normalize_score(70 - ((total_tokens / label_size) - total_input_ngrams))
                    self._matches.add(some_key=label,
                                      some_match=pattern,
                                      some_type="ldm",
                                      some_sub_type="s-tuple",
                                      some_confidence=score)

                # try a stemmed tuple
                elif self.has_exact_values(pattern,
                                           stemmed_ngrams):
                    score = self._normalize_score(80 - ((total_tokens / label_size) - total_input_ngrams))
                    self._matches.add(some_key=label,
                                      some_match=pattern,
                                      some_type="ldm",
                                      some_sub_type="e-tuple",
                                      some_confidence=score)

                # try the normal tuple
                elif self.has_startswith_values(pattern,
                                                the_input_ngrams):
                    score = self._normalize_score(90 - ((total_tokens / label_size) - total_input_ngrams))
                    self._matches.add(some_key=label,
                                      some_match=pattern,
                                      some_type="ldm",
                                      some_sub_type="n-tuple",
                                      some_confidence=score)

        if self._is_debug:
            if len(self._matches.get_matches()) > 0:
                self.logger.debug("\n".join([
                    "Long Distance Matches",
                    pprint.pformat(self._matches.get_matches(), indent=4)]))
            else:
                self.logger.debug("No Long Distance Matches")
