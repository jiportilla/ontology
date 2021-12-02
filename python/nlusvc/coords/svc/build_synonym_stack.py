#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import itertools

from base import BaseObject
from datadict import FindSynonym


class BuildSynonymStack(BaseObject):
    """  """

    def __init__(self,
                 ontology_name: str,
                 is_debug: bool = False):
        """
        Created:
            14-Jan-2020
            craig.trim@ibm.com
        """
        BaseObject.__init__(self, __name__)

        self._is_debug = is_debug
        self._finder = FindSynonym(is_debug=self._is_debug,
                                   ontology_name=ontology_name)

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Initialize BuildSynonymStack",
                f"\tOntology Name: {ontology_name}"]))

    def _build_token_variants(self,
                              a_term: str) -> list:
        """
        Purpose:
        Sample Input:
            White Blood Cell
        Synonyms:
            White Blood Cell~Immune Cell
            Cell~Cells
        Sample Output
            [White Blood Cell, White Blood Cells]
        :param a_term:
        :return:
        """

        master = []
        for token in a_term.split():
            syns = self._finder.synonyms(token, known=False)
            if syns:
                master.append(syns)
            else:
                master.append([token])

        if not len(master):
            return []

        cartesian = []
        for element in itertools.product(*master):
            cartesian.append(element)

        patterns = sorted([' '.join(x) for x in cartesian])
        return patterns

    def process(self,
                term: str):
        """
        Sample Input:
                'test_syn_1'
        Sample Output:
            [   'test syn 1', 'test syn 2', 'test_syn_2']
        :param term:
        :return:
        """

        s_patterns = set()
        leaf_patterns = []

        def build_stack(some_term: str) -> None:
            s_patterns.add(some_term)
            variants = self._finder.synonyms(some_term, known=False)

            if not variants:
                leaf_patterns.append(some_term)
                return

            variants = [x for x in variants if x != some_term]
            if self._is_debug:
                self.logger.debug(f"Term: {some_term}, Variants={variants}")

            [build_stack(variant) for variant in variants if variant not in s_patterns]

        build_stack(term)

        def build_token_variants():
            m = set()
            for x in s_patterns:
                [m.add(z) for z in self._build_token_variants(x)]
            return sorted(set([x.lower().strip() for x in m]))

        svcresult = {
            "patterns": build_token_variants(),
            "resolution": leaf_patterns}

        if self._is_debug:
            self.logger.debug('\n'.join([
                f"Synonym Reveral Complete (term={term})",
                f"\tPatterns: {svcresult['patterns']}",
                f"\tResolution: {svcresult['resolution']}"]))

        return svcresult
