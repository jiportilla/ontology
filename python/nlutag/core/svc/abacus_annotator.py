#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pprint

from spacy.tokens import Doc

from base import BaseObject
from nlutag.core.dmo import ExactEntityMatcher
from nlutag.core.dmo import LongDistanceMatcher
from nlutag.core.dto import TokenMatches


class AbacusAnnotator(BaseObject):

    def __init__(self,
                 doc: Doc,
                 d_input: dict,
                 ontology_name: str,
                 is_debug: bool = False):
        """
        Updated:
            12-Apr-2017
            craig.trim@ibm.com
            *   rename from "CitiProductAnnotator"
        Updated:
            28-Jul-2017
            craig.trim@ibm.com
            *   renamed from 'CitiAnnotator'
                <https://github.ibm.com/abacus-implementation/Abacus/issues/1637#issuecomment-3046334>
        Updated:
            21-Feb-2019
            craig.trim@ibm.com
            *   migrated from -text
        Updated:
            25-Apr-2019
            craig.trim@ibm.com
            *   modified instantiation strategy
        Updated:
            21-Aug-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/796
                -   merged with 'find-entity-matches'
                -   integrate with spaCy doc
        Updated:
            22-Aug-2019
            craig.trim@ibm.com
            *   keep 'token-matches' at the process(...) level rather than class level
                at the class level this will propogate caching (act as a singleton)
        Updated:
            13-Dec-2019
            craig.trim@ibm.com
            *   use ontology-name as a param
                https://github.ibm.com/GTS-CDO/unstructured-analytics/pull/1587
        :param doc:
            a spaCy doc representing the original input
        :param d_input:
            Sample Structure:
                {   'original':     '...',
                    'normalized':   '...',
                    'tokens': {
                        'normalized':   [..],
                        'skipgrams':    [..],
                        'ngrams': {
                            'gram1':    [..],
                            'gram2':    [..],
                            'gram3':    [..]}}}
        """
        BaseObject.__init__(self, __name__)
        self._doc = doc
        self._d_input = d_input
        self._ontology_name = ontology_name

        self._is_debug = is_debug
        if self._is_debug:
            self.logger.debug('\n'.join([
                "Instantiate AbacusAnnotator",
                f"\tOntology Name: {ontology_name}"]))

    def process(self) -> TokenMatches:
        """
        Purpose:
            Perform token matching
        :return:
            a populated TokenMatches structure
        """

        token_matches = TokenMatches()

        ExactEntityMatcher(self._d_input,
                           token_matches,
                           is_debug=self._is_debug,
                           ontology_name=self._ontology_name).process()

        LongDistanceMatcher(doc=self._doc,
                            is_debug=self._is_debug,
                            some_input=self._d_input,
                            some_matches=token_matches,
                            ontology_name=self._ontology_name).process()

        if self._is_debug and len(token_matches.get_matches()):
            self.logger.debug("\n".join([
                "Matching Completed",
                pprint.pformat(token_matches.get_matches(), indent=4)]))

        return token_matches
