#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import itertools
import pprint
from typing import Optional

from spacy.lang.en import English

from base import BaseObject


class CoordMatcherToken(BaseObject):
    """ Provide (X,Y) Coordinates for Location of Entity Tokens in an Input String """

    __nlp = English()

    def __init__(self,
                 input_text: str,
                 d_tokenized_input: dict,
                 entity_text: str,
                 d_tokenized_entity: dict,
                 is_debug: bool = False):
        """
        Created:
            10-Jan-2020
            craig.trim@ibm.com
            *   find match coordinates for exact entity matches
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1722
        Updated:
            13-Jan-2020
            craig.trim@ibm.com
            *   renamed from 'coordinate-extractor-normalized'
        Updated:
            15-Jan-2020
            craig.trim@ibm.com
            *   renamed from 'tokenized-coord-matcher'
        :param input_text:
            The normalized form of the utterance being analyzed
            Sample Input:
                'and cells killer the cytotoxic natural activity of natural killer (NK) cells and cells and natural kill memory CD8+ T cells'
        :param entity_text:
            the entity text to match on
            this text will be tokenized and compared to the tokenized form of the input text
        """
        BaseObject.__init__(self, __name__)

        self._is_debug = is_debug
        self._input_text = input_text
        self._entity_text = entity_text
        self._d_tokenized_input = d_tokenized_input
        self._d_tokenized_entity = d_tokenized_entity

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Initialize CoordMatcherToken",
                f"\tInput Text: {self._input_text}",
                f"\tInput Tokens: {d_tokenized_input}",
                f"\tEntity Text: {self._entity_text}",
                f"\tEntity Tokens: {d_tokenized_entity}"]))

    def _normalize(self,
                   some_text: str) -> dict:
        from nlusvc.coords.dmo import CharacterLevelTokenizer

        return CharacterLevelTokenizer(input_text=some_text,
                                       is_debug=self._is_debug).process()

    def _tokenize(self,
                  some_text: str) -> list:

        doc = self.__nlp(some_text)
        tokens = []
        for token in doc:
            tokens.append(token.text)

        tokens = [x.replace('\n', '').strip() for x in tokens if x]
        tokens = [x.lower().strip() for x in tokens if x and len(x)]

        return tokens

    @staticmethod
    def _candidate_token_sequences(pattern_tokens: list,
                                   normalized_tokens: list) -> list:
        """
        Purpose:
            Find a list of candidate token sequences within the normalized_input_text
        :param pattern_tokens:
            a list of 1..* tokens forming a match pattern
        :param normalized_tokens:
            the tokenized form of the normalized_input_text
        :return:
            a list of possible token sequences
            Sample Output:
                [   (5, 2, 1), (5, 2, 11),
                    (5, 9, 11), (5, 9, 13),
                    ...
                    (8, 9, 11), (8, 9, 13),
                    (15, 9, 11), (15, 9, 13) ]
        """

        def find_index_position(a_pattern_token: str) -> list:
            m = []
            for index in range(0, len(normalized_tokens)):
                if a_pattern_token.lower() == normalized_tokens[index].lower():
                    m.append(index)
            return m

        matches = []
        [matches.append(find_index_position(pattern_token))
         for pattern_token in pattern_tokens]

        cartesian = []
        for element in itertools.product(*matches):
            cartesian.append(element)

        return cartesian

    @staticmethod
    def _filter_candidate_sequences(candidates: list) -> list:
        """
        Purpose:
            Discard candidate sequences that fail to meet criteria
        :param candidates:
            Sample Input:
                [   (5, 2, 1), (5, 2, 11),
                    (5, 9, 11), (5, 9, 13),
                    ...
                    (8, 9, 11), (8, 9, 13),
                    (15, 9, 11), (15, 9, 13) ]
        :return:
            Sample Output:
                [   (5, 9, 11), (5, 9, 13),
                    (8, 9, 11), (8, 9, 13)]
        """

        def is_continous_order(seq: list) -> bool:
            """
            Criteria:
                A token match sequence must be in continuous order

                Valid Sequence:
                    [8, 9, 11]
                Invalid Sequence:
                    [8, 3, 11]

            :param seq:
                a given sequence
            :return:
                True        sequence is valid
                False       sequence is invalid
            """
            for i in range(0, len(seq)):
                if i + 1 >= len(seq):
                    return True
                elif seq[i] > seq[i + 1]:
                    return False

            return True

        return [x for x in candidates
                if is_continous_order(list(x))]

    @staticmethod
    def _locate_optimal_sequence(candidates: list) -> Optional[list]:
        """
        Purpose:
            Find the sequence with tightly clustered numbers (lowest delta)
        :param candidates:
            Sample Input:
                [   (5, 9, 11), (5, 9, 13),
                    (8, 9, 11), (8, 9, 13)]
        :return:
            Sample Output:
                [   8, 9, 11]
        """

        def add_deltas(seq: list) -> int:
            total = 0
            for i in range(0, len(seq)):
                if i + 1 >= len(seq):
                    return total
                total += (seq[i + 1] - seq[i])
            return total

        d_result = {}
        for a_sequence in candidates:
            delta = add_deltas(list(a_sequence))
            d_result[delta] = a_sequence

        if d_result:
            return list(d_result[min(d_result.keys())])

    def _perform_match(self,
                       optimal_sequence: list) -> dict:
        from nlusvc.coords.dto import MatchStructure

        match_pattern = []
        for i in range(optimal_sequence[0], optimal_sequence[-1] + 1):
            match_pattern.append(self._d_tokenized_input[i]['original'])

        s_match_pattern = ''.join(match_pattern)

        x = self._input_text.index(s_match_pattern)
        y = x + len(''.join(s_match_pattern))
        match_text = self._input_text[x:y].strip()

        if match_text.endswith('.'):  # GIT-1732-17146964
            match_text = match_text[:-1]

        return MatchStructure.generate(input_string=self._input_text,
                                       entity_text=self._entity_text,
                                       match_text=match_text,
                                       x=x, y=y)

    def process(self) -> Optional[dict]:
        """
        Purpose:
            Provide (x, y) coordinates for a long-distance text match
        :return:
        """

        normalized_pattern_tokens = [self._d_tokenized_entity[x]['normalized']
                                     for x in self._d_tokenized_entity]
        normalized_input_tokens = [self._d_tokenized_input[x]['normalized']
                                   for x in self._d_tokenized_input]

        candidates = self._candidate_token_sequences(pattern_tokens=normalized_pattern_tokens,
                                                     normalized_tokens=normalized_input_tokens)
        if not candidates:
            return None
        elif self._is_debug:
            self.logger.debug(f"Candidate Token Sequences: {candidates}")

        candidates = self._filter_candidate_sequences(candidates)
        if not candidates:
            return None
        elif self._is_debug:
            self.logger.debug(f"Filtered Candidate Sequences: {candidates}")

        optimal = self._locate_optimal_sequence(candidates)
        if not optimal:
            return None
        elif self._is_debug:
            self.logger.debug(f"Optimal Sequence: {optimal}")

        svcresult = self._perform_match(optimal)
        if self._is_debug:
            self.logger.debug('\n'.join([
                "Long Distance Coordinate Match Found",
                f"\tCandidate Sequences: {candidates}",
                pprint.pformat(svcresult, indent=4)]))

        return svcresult
