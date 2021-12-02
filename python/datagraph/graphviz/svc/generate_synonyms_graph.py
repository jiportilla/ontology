# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from datadict import FindSynonym


class GenerateSynonymsGraph(BaseObject):
    """
    Purpose:
    Service that creates a Graphviz diagram exploring Language Variability in the Synonyms file

    Traceability:
    https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1095

    Sample Input:
        [   'Public Blockchain' ]

    Sample Output:
        Public Blockchain -- blockchain public
        blockchain public -- blockchain
        blockchain -- block chain
        block chain -- block
        block -- blocked
        block -- blocking
        block -- blocks
        block -- prevent
        block chain -- chain
        chain -- chained
        chain -- chainer
        chain -- chainers
        chain -- chaining
        chain -- chains
        blockchain -- blockchain
        blockchain -- blockchain solution
        blockchain solution -- solution
        solution -- it solution
        it solution -- it
        solution -- soloution
        solution -- soloutions
        solution -- solutioned
        solution -- solutioner
        solution -- solutioners
        solution -- solutioning
        solution -- solutions
        blockchain -- blockchaining
        blockchain -- blockchains
        blockchain -- main chain
        main chain -- main
        blockchain -- mainchain
        blockchain public -- public
        public -- are readable by the public
        are readable by the public -- are
        are -- ar
        are -- that are
        that are -- that
        are readable by the public -- readable
        are readable by the public -- by
        are readable by the public -- the
        the -- te
        the -- teh
        public -- readable by the public
        public -- that public
        Public Blockchain -- public blockchain

    Prereq:
    None
    """

    def __init__(self,
                 synonyms: list,
                 ontology_name: str = 'base',
                 is_debug: bool = True):
        """
        Created:
            11-Oct-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1095
        Updated:
            13-Dec-2019
            craig.trim@ibm.com
            *   load dictionaries by ontology name
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1582
        :param is_debug:
            if True     increase log output at DEBUG level
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._synonyms = synonyms
        self._synonym_finder = FindSynonym(is_debug=is_debug,
                                           ontology_name=ontology_name)

    def process(self,
                recursive: bool = True) -> list:
        rels = []
        s_unique = set()

        def find(an_input: str):
            def _canon():
                c = self._synonym_finder.canon(an_input)
                if c:
                    return c
                return an_input

            syns = self._synonym_finder.synonyms(_canon())
            if not syns:
                return

            for syn in syns:

                if an_input != syn:
                    rels.append(f"{an_input} -- {syn}")

                if recursive and ' ' in syn:
                    for inner_syn in syn.split(' '):
                        if inner_syn in s_unique:
                            continue
                        s_unique.add(inner_syn)

                        if syn != inner_syn:
                            rels.append(f"{syn} -- {inner_syn}")
                        find(inner_syn)

        for input_syn in self._synonyms:
            find(input_syn)

        [print(x) for x in rels]
        return rels
