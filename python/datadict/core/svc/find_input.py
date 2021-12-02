# !/usr/bin/env python
# -*- coding: UTF-8 -*-


import textacy

from base import BaseObject


class FindInput(BaseObject):
    """
    performs the simplest check possible

    does the input exist somewhere?

    This component says nothing about the input being a pattern, an ontological label, a synonym or a go-word

    an item of input is either known (exists in the KB) or it doesn't
     """

    def __init__(self,
                 ontology_name: str = 'base',
                 is_debug: bool = False):
        """
        Created:
            9-Apr-2019
            craig.trim@ibm.com
        Updated:
            21-Apr-2019
            craig.trim@ibm.com
            *   added 'nearest' (and sequential algorithms)
        Updated:
            13-Dec-2019
            craig.trim@ibm.com
            *   load dictionaries by ontology name
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1582
        """
        BaseObject.__init__(self, __name__)
        from datadict.core.svc import FindEntity
        from datadict.core.svc import LoadStopWords

        self._is_debug = is_debug
        self._ontology_name = ontology_name

        self._gowords = self._gowords()
        self._synonyms = self._load_syns()
        self._stopwords = LoadStopWords(is_debug=is_debug).load()
        self._entity_finder = FindEntity(is_debug=is_debug,
                                         ontology_name=ontology_name)

    def _load_syns(self):
        from datadict.core.svc import FindSynonym

        syn_finder = FindSynonym(is_debug=self._is_debug,
                                 ontology_name=self._ontology_name)
        return syn_finder.all(lower=True,
                              keep_regexp=False,
                              transform_spans=True)

    @staticmethod
    def _gowords() -> list:
        return []

    def _normalize(self,
                   text: str) -> str:
        """
        :param text:
        :return:
            a normalized copy of the string
        """
        text = textacy.preprocess_text(text,
                                       fix_unicode=False,
                                       lowercase=False,
                                       # transliterate=True,
                                       no_urls=True,
                                       no_emails=True,
                                       no_phone_numbers=True,
                                       no_numbers=True,
                                       no_currency_symbols=True,
                                       no_punct=True,
                                       no_contractions=True,
                                       no_accents=True)
        tokens = text.lower().strip().split(" ")
        tokens = [x for x in tokens if x not in self._stopwords]

        return " ".join(tokens)

    def nearest(self,
                some_input: str) -> dict:

        normalized_input = self._normalize(some_input)

        if normalized_input != some_input:
            if self.exists(normalized_input):
                return {normalized_input: 90}

    def exists(self,
               some_input: str) -> bool:
        """
        :param some_input:
        :return:
            False       the input is not known in any file or any aspect of the knowledge base
        """
        some_input = some_input.lower().strip()
        if self._entity_finder.label(some_input):
            return True  # is a label
        if some_input in self._gowords:
            return True  # is goword
        if some_input in self._stopwords:
            return True  # is stopword
        if some_input in self._synonyms:
            return True  # is synonym

        return False
