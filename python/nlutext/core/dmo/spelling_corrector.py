#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from datadict import FindEntity
from datadict import FindStopword
from datadict import FindSynonym

ACRONYM_THRESHOLD: int = 6


class SpellingCorrector(BaseObject):
    """ Spelling Corrector """

    def __init__(self,
                 some_tokens,
                 ontology_name: str = 'base',
                 is_debug: bool = False):
        """
        Created:
            24-Mar-2017
            craig.trim@ibm.com
        Updated:
            4-Apr-2017
            craig.trim@ibm.com
            *   this is a computationally expensive component
                adds an average of [1.5 - 2.0] seconds per call
            *   add unigram check
        Updated:
            13-Apr-2017
            craig.trim@ibm.com
            *   added 'self.has_digit' check
            *   added 'FindEntity' check
        Updated:
            19-Apr-2017
            craig.trim@ibm.com
            *   removed use of
                from autocorrect import spell
                as Xavier rightly points out,
                when a mobile phone can spell correct an entire text message in an instant,
                the +4-5s this module was taking just doesn't make sense
                The 'autocorrect' has this odd habit of loading the entire dictionary into memory
                each time this component is invoked
            *   using the Norvig style checker now
            *   renamed from "PerformSpellCorrection"
        Updated:
            9-Jan-2018
            craig.trim@ibm.com
            *   added FindAcronym lookup within 'is-known-entity'
        Updated:
            15-Fev-2018
            xavier.verges@es.ibm.com
            *   bulk check for acronyms
        Updated:
            2-Apr-2018
            craig.trim@ibm.com
            *   moved staticmethods to separate class (for consistency)
            *   added 'ACRONYM_THRESHOLD'
                any term greater than this length is assumed to NOT be an acronym
        Updated:
            21-Feb-2019
            craig.trim@ibm.com
            *   migrated to text
        Updated:
            26-Mar-2019
            craig.trim@ibm.com
            *   changes to 'find-entity' based on MDA changes
        Updated:
            13-Dec-2019
            craig.trim@ibm.com
            *   removed find-acronym service; the underlying data file was empty
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1580
        :param some_tokens:
        """
        BaseObject.__init__(self, __name__)

        self._is_debug = is_debug
        self._tokens = some_tokens
        self._ontology_name = ontology_name

        self._alphabet = self.get_alphabet()
        self._entity_finder = FindEntity(is_debug=is_debug,
                                         ontology_name=ontology_name)
        self._syn_finder = FindSynonym(is_debug=is_debug,
                                       ontology_name=ontology_name)

    def edits(self, word):
        """
        Reference:
            Peter Norvig style spell checker
            <http://stackoverflow.com/questions/3449968/large-scale-spell-checking-in-python>
        :param word:
        :return:
        """
        s = [(word[:i], word[i:]) for i in range(len(word) + 1)]
        deletes = [a + b[1:] for a, b in s if b]
        transposes = [a + b[1] + b[0] + b[2:] for a, b in s if len(b) > 1]
        replaces = [a + c + b[1:] for a, b in s for c in self._alphabet if b]
        inserts = [a + c + b for a, b in s for c in self._alphabet]
        return list(set(deletes + transposes + replaces + inserts))

    def get_correct_spelling(self, token):
        """
        Purpose:
            computationally efficient spell correction

            Rules:
                1.  If a token does not exist in either Entities or Synonyms
                2.  Generate Norvig Variations
                3.  Return the first variation that exists in either Entities or Synonyms
        :param token:
        :return:
            correctly spelled token
            or the original token
        """
        edits = self.edits(token)

        corrected = self.get_known_entity(token, edits)
        if corrected:
            return corrected

        return token

    def is_known_entity(self,
                        token: str):
        is_synonym = self._syn_finder.exists(token)
        if is_synonym:
            return True

        is_entity = self._entity_finder.exists(token)
        if is_entity:
            return True

        is_stopword = FindStopword(token).exists()
        if is_stopword:
            return True

        return False

    def get_known_entity(self,
                         token,
                         candidates):
        for token in candidates:
            if (self._syn_finder.exists(token) or
                    self._entity_finder.exists(token) or
                    FindStopword(token).exists()):
                return token

        return None

    @staticmethod
    def has_digit(some_input):
        return any(i.isdigit() for i in some_input)

    @staticmethod
    def get_alphabet():
        import string
        return list(string.ascii_lowercase)

    def process(self):
        normalized_tokens = []
        for token in self._tokens:

            # only spell correct on unigrams
            if len(token.split(" ")) > 1:
                normalized_tokens.append(token)
                continue

            is_known_entity = self.is_known_entity(token)

            if is_known_entity:
                normalized_tokens.append(token)
                continue

            # don't spell correct on tokens with digits
            if self.has_digit(token):
                normalized_tokens.append(token)
                continue

            token = self.get_correct_spelling(token)
            normalized_tokens.append(token)

        if self._is_debug and normalized_tokens != self._tokens:
            self.logger.debug("\n".join([
                "Spelling Correction Complete: ",
                f"\tOntology Name: {self._ontology_name}",
                f"\tOriginal: {self._tokens}",
                f"\tNormalized: {normalized_tokens}"]))

        return normalized_tokens
