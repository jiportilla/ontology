#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pprint
from typing import Optional

from base import BaseObject
from datadict import FindSynonym


class PerformCoordExtraction(BaseObject):
    """ Provide (X,Y) Coords for Location of Entity Text in an Input String """

    def __init__(self,
                 ontology_name: str,
                 is_debug: bool = False):
        """
        Created:
            10-Jan-2020
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1722
        Updated:
            14-Jan-2020
            craig.trim@ibm.com
            *   refactoring and simplification based on
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1732
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._ontology_name = ontology_name
        self._syn_finder = FindSynonym(is_debug=self._is_debug,
                                       ontology_name=ontology_name)

    def _match_tokens(self,
                      input_text: str,
                      d_tokenized_input: dict,
                      entity_text: str,
                      d_tokenized_entity: dict) -> Optional[dict]:
        from nlusvc.coords.dmo import CoordMatcherToken

        svcresult = CoordMatcherToken(entity_text=entity_text,
                                      input_text=input_text,
                                      d_tokenized_input=d_tokenized_input,
                                      d_tokenized_entity=d_tokenized_entity,
                                      is_debug=self._is_debug).process()

        if self._is_debug and svcresult:
            self.logger.debug('\n'.join([
                f"Coordinate Extraction Completed",
                f"\tStep: Match Tokens",
                pprint.pformat(svcresult, indent=4)]))

        return svcresult

    def _match_entity(self,
                      input_text: str,
                      entity_text: str) -> Optional[dict]:
        """
        Purpose:
            A Näive Simple Text Matcher
        :param input_text:
            the user input string
        :param entity_text:
            the entity we're trying to match
        :return:
        """
        from nlusvc.coords.dmo import CoordMatcherText

        svcresult = CoordMatcherText(is_debug=self._is_debug,
                                     input_text=input_text,
                                     entity_text=entity_text).process()

        if self._is_debug and svcresult:
            self.logger.debug('\n'.join([
                f"Coordinate Extraction Completed",
                f"\tStep: Match Entity",
                pprint.pformat(svcresult, indent=4)]))

        return svcresult

    def _synonyms(self,
                  a_term: str) -> list:
        from nlusvc.coords.svc import BuildSynonymStack
        builder = BuildSynonymStack(is_debug=self._is_debug,
                                    ontology_name=self._ontology_name)

        return builder.process(a_term)['patterns']

    @staticmethod
    def _tokenize(some_input: str) -> dict:
        from nlusvc.coords.dmo import CharacterLevelTokenizer
        svc = CharacterLevelTokenizer(input_text=some_input,
                                      is_debug=False)

        return svc.process()

    @staticmethod
    def _preprocess_input(input_text: str):
        """
        Purpose:
            Light-weight Preprocessing on Input
        Reference:
             GIT-1732-17146674
        :param input_text:
            the input text
        :return:
            the preprocessed input tet
        """
        input_text = input_text.replace('\n', ' ')
        input_text = input_text.replace('\t', ' ')
        while '  ' in input_text:
            input_text = input_text.replace('  ', ' ')

        return input_text

    def _perform_match(self,
                       input_text: str,
                       entity_text: str) -> list:
        results = []

        # Simple Match on Input
        svcresult = self._match_entity(input_text=input_text,
                                       entity_text=entity_text)
        if svcresult:
            return [svcresult]

        patterns = self._synonyms(entity_text)

        d_tokenized_input = self._tokenize(input_text)
        if self._is_debug:
            self.logger.debug('\n'.join([
                "Tokenized Input Text",
                pprint.pformat(d_tokenized_input)]))

        for pattern in patterns:
            svcresult = self._match_entity(input_text=input_text,
                                           entity_text=pattern)
            if svcresult:
                results.append(svcresult)

            # Simple Match on Tokenized Input
            d_tokenized_entity = self._tokenize(pattern)
            if self._is_debug:
                self.logger.debug('\n'.join([
                    f"Tokenized Pattern Text (pattern='{pattern}')",
                    pprint.pformat(d_tokenized_entity)]))

            svcresult = self._match_tokens(input_text=input_text,
                                           entity_text=pattern,
                                           d_tokenized_input=d_tokenized_input,
                                           d_tokenized_entity=d_tokenized_entity)
            if svcresult:
                results.append(svcresult)

        return results

    def _choose_result(self,
                       results: list) -> Optional[dict]:

        d_spans = {}
        for result in results:
            span = result['y'] - result['x']
            if span not in d_spans:
                d_spans[span] = []
            d_spans[span].append(result)

        results = d_spans[max(d_spans.keys())]

        if len(results) == 0:
            self.logger.error('\n'.join([
                "Unexpected Situation: Further Investigation Required",
                pprint.pformat(d_spans, indent=4)]))
            raise ValueError

        if len(results) == 1:
            return results[0]

        if self._is_debug:
            self.logger.warning('\n'.join([
                f"Multiple Results (total={len(results)})",
                f"\tReturning First Result Only",
                pprint.pformat(results, indent=4)]))

        return results[0]

    def process(self,
                input_text: str,
                entity_text: str) -> Optional[dict]:
        """
        Purpose:
            Extract the (x,y) Coordinates of an Entity in the Input Text
        Sample Input:
            Input Text:
                "and the cytotoxic activity of natural killer (NK) cells"
            Entity Text:
                "Natural Killer Cell"
        Sample Output:
            {   'x': 30, y: '55',
                'substring': 'natural killer (NK) cells' }
        :param input_text:
            any input text in an original state (non-normalized)
        :param entity_text:
            any extracted (Ontology) entity
        :return:
            a dictionary of the (x,y) coordinates
        """
        input_text = self._preprocess_input(input_text)

        results = self._perform_match(input_text=input_text,
                                      entity_text=entity_text)

        if not results or not len(results):
            if self._is_debug:
                self.logger.debug('\n'.join([
                    "Coordinate Extraction Failure",
                    f"\tEntity: {entity_text}",
                    f"\tInput Text: {input_text}"]))
            return None

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Coordinate Extraction Candidates",
                pprint.pformat(results, indent=4)]))

        svcresult = self._choose_result(results)

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Coordinate Extraction Completed",
                pprint.pformat(svcresult, indent=4)]))

        return svcresult
