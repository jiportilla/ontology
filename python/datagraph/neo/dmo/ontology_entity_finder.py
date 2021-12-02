#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from rdflib.term import Literal

from base import BaseObject
from base import MandatoryParamError


class OntologyEntityFinder(BaseObject):
    """ find an entity in the skills ontology """

    def __init__(self, the_ontology_graph):
        """
        Created:
            21-Feb-2019
            craig.trim@ibm.com
        :param the_ontology_graph:
            the ontology graph loaded from another source
        """
        BaseObject.__init__(self, __name__)
        if not the_ontology_graph:
            raise MandatoryParamError("Ontology Graph")

        self.graph = the_ontology_graph

    def all(self) -> list:
        qres = self.graph.query("""
            SELECT
                ?s_label ?p ?o_label
            WHERE {
                ?s rdfs:label ?s_label .
                ?o rdfs:label ?o_label .
                ?s ?p ?o .
            }
        """)

        def _s(a_row) -> Literal:
            return a_row[0]

        def _p(a_row) -> str:
            return str(a_row[1]).split("#")[-1].strip().lower()

        def _o(a_row) -> Literal:
            return a_row[2]

        records = []
        for row in qres:
            s = _s(row).title()
            p = _p(row)
            o = _o(row).title()

            records.append((s, p, o))

        return records

    def _top_level_parent(self, some_entity):
        pass

    def unlabelled(self) -> list:

        qres = self.graph.query("""
            SELECT
                ?s ?label
            WHERE {
                ?s ?p ?o .
                OPTIONAL {
                    ?s rdfs:label ?label .
                } .
            }
        """)

        properties = [
            'ancestorof', 'ancestorof', 'contains', 'owns', 'partof', 'implies'
        ]

        def _s(a_row) -> str:
            return a_row[0].title().split("#")[-1].strip().lower()

        def _has_label(a_row) -> str:
            return a_row[1]

        def _is_valid(candidate):
            if "/" in candidate:
                return False
            if candidate in properties:
                return False
            return True

        unlabelled = [_s(row) for row in qres if not _has_label(row)]
        unlabelled = sorted(set([x for x in unlabelled if _is_valid(x)]))

        return unlabelled
