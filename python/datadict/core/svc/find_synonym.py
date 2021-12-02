# !/usr/bin/env python
# -*- coding: UTF-8 -*-


import re
import time
from typing import Optional

from base import BaseObject

PROV_WILDCARD = "synonyms"


class FindSynonym(BaseObject):
    """ Act as a controller in front of all synonym dictionaries """

    __syns = None
    __by_word_length = None
    __seealso_lcase = None
    __longest_ngram = 0

    def __init__(self,
                 ontology_name: str = 'base',
                 is_debug: bool = False):
        """
        Created:
            24-Mar-2017
            craig.trim@ibm.com
        Updated:
            2-Aug-2017
            craig.trim@ibm.com
            *   use config-based provenance
                https://github.ibm.com/abacus-implementation/Abacus/issues/1721#issuecomment-3080923
        Updated:
            27-Sep-2017
            craig.trim@ibm.com
            *   migrate to abacus-att
                remove any references to flows
        Updated:
            21-Feb-2019
            craig.trim@ibm.com
            *   migrated to text
        Updated:
            11-April-2019
            anassar@us.ibm.com
            *   add reverse synonym dictionary
        Updated:
            7-Aug-2019
            craig.trim@ibm.com
            *   use singleton and lazy loading for synonyms dictionary
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/661
        Updated:
            27-Sept-2019
            craig.trim@ibm.com
            *   integrate 'see-also' capabilities
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1020
        Updated:
            11-Oct-2019
            craig.trim@ibm.com
            *   reversed changes from 7-Aug-2019
        Updated:
            22-Oct-2019
            xavier.verges@es.ibm.com
            *   enable to look for synonyms of a given word length
        Updated:
            13-Dec-2019
            craig.trim@ibm.com
            *   load dictionaries by ontology name
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1582
            *   remove reverse-regex-syns
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1583#issuecomment-16612774
        """
        BaseObject.__init__(self, __name__)
        from datadict.core.dmo import DictionaryLoader

        start = time.time()
        self._is_debug = is_debug

        _loader = DictionaryLoader(ontology_name=ontology_name)

        self._d_see_also = _loader.synonyms().seeAlso()
        self._d_rev_synonyms = _loader.synonyms().rev()

        if not self.__syns:
            self._build_static_dictionaries(_loader.synonyms().fwd())

        total_time = str(time.time() - start)[:4]

        if is_debug:
            self.logger.debug('\n'.join([
                "Initialized FindSynonym",
                f"\tTime: {total_time}s",
                f"\tOntology Name: {ontology_name}"]))

    def _build_static_dictionaries(self,
                                   d_synonyms: dict):
        if self._is_debug:
            self.logger.debug("Loading Synonyms Dictionary")
        self.__syns = d_synonyms
        self.__by_word_length = {'with_regexp': {}}

        for key, values in d_synonyms.items():
            key = key.replace(',', '')
            for value in values:
                if '[' in value:
                    self._update_regexp_dictionary(self.__by_word_length['with_regexp'],
                                                   value,
                                                   key)
                else:
                    word_length = len(value.split())
                    if not word_length in self.__by_word_length:
                        self.__by_word_length[word_length] = {}
                    if not key in self.__by_word_length[word_length]:
                        self.__by_word_length[word_length][key] = []
                    self.__by_word_length[word_length][key].append(value)
                    self.__longest_ngram = max(self.__longest_ngram, word_length)
        for word_length in range(1, self.__longest_ngram + 1):
            if word_length not in self.__by_word_length:
                self.__by_word_length[word_length] = {}

    @staticmethod
    def _update_regexp_dictionary(regexp_dict,
                                  pattern,
                                  replacement):
        if "(?:^" not in pattern:
            pattern = f'(?:^|\\s){pattern}'
        if "(?:$" not in pattern:
            pattern = f'{pattern}(?:$|\\s)'
        compiled = re.compile(pattern)
        regexp_dict[compiled] = f' {replacement} '

    def dict(self) -> dict:
        return self.__syns

    def all(self,
            lower: bool = True,
            keep_regexp: bool = False,
            transform_spans: bool = True) -> list:
        """
        Purpose:
            return all words in the synonyms list

        :param lower:
            force lowercase
        :param keep_regexp:
            return regular expressions
        :param transform_spans:
            transform word span "alpha_beta_gamma" to "alpha beta gamma"
        """
        words = set()

        for k in self.dict():
            words.add(k)
            for value in self.dict()[k]:
                words.add(value)

        if lower:
            words = [x.lower().strip() for x in words]

        if not keep_regexp:
            words = [x for x in words if "[" not in x]
            words = [x for x in words if "+" not in x]

        if transform_spans:
            words = [x.replace("_", " ") for x in words]

        return sorted(words)

    def canon(self,
              some_input: str) -> str:
        """
        Purpose:
            Find the Canonical form for a Synonym
        :param some_input:
            some term
        :return:
            the canonical form
        """
        _input = some_input.lower()
        if _input in self._d_rev_synonyms:
            return self._d_rev_synonyms[_input]

        if ' ' in _input:
            canon_form = ' '.join([self.canon(x) for x in _input.split(' ')])
            if canon_form != _input:
                return canon_form

    def keys_in_swap_level(self,
                           swap_level: int):
        return self.__by_word_length[swap_level].keys()

    def synonyms_in_swap_level(self,
                               known_key: str,
                               swap_level: int):
        return self.__by_word_length[swap_level][known_key]

    def regexps_with_synonyms(self):
        return self.__by_word_length['with_regexp']

    def max_ngram(self) -> int:
        return self.__longest_ngram

    def synonyms(self,
                 some_input: str,
                 known: bool = False) -> Optional[list]:
        """
        Purpose:
            Given a head (canonical) form, find the synonyms (variations)
        :param some_input:
            any head (canonical) form
            e.g., 'Project Manager'
        :param known:
            indicates the input parameter is a known dictionary key
            this is an optimization that places the burden of exception handling on the caller
        :return:
            a list of synonyms (variations)
            e.g., [ 'PM',
                    'Proj. Mgr' ]
        """
        if known:
            return self.dict()[some_input]

        some_token = some_input.lower().strip()
        if some_token in self.dict():
            return self.dict()[some_token]

        if ' ' in some_token:
            some_token = some_token.replace(" ", "_")
            if some_token in self.dict():
                return self.dict()[some_token]

    def see_also(self,
                 some_input: str) -> list:
        """

        :param some_input:
        :return:
        """

        def _cleanse(a_token: str) -> str:
            return a_token.lower().strip()

        for k in self._d_see_also:
            if _cleanse(k) == _cleanse(some_input):
                return self._d_see_also[k]
        return []

    def exists(self,
               some_input: str) -> bool:
        """
        :param some_input:
            any head (canonical) form to the dictionary
            NOTE:   No string manipulation is performed for this lookup
                    the input fAorm is case sensitive
        :return:
            True        if the input exists as a dictionary key
            False       the input does not exist as a dictionary key
        """
        return self.synonyms(some_input) is not None
