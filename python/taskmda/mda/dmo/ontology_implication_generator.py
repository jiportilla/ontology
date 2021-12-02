#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from rdflib import Graph
from rdflib.plugins.sparql.processor import SPARQLResult

from base import BaseObject
from base import MandatoryParamError


class OntologyImplicationGenerator(BaseObject):
    def __init__(self,
                 some_graph: Graph):
        """
        Created:
            27-Feb-2019
            craig.trim@ibm.com
            *   refactored out of generate-ontology-dict
        """
        BaseObject.__init__(self, __name__)
        if not some_graph:
            raise MandatoryParamError("Ontology Graph")

        self.graph = some_graph

    def _ontology_query(self) -> dict:
        d = {}

        def _update_dict(query_results: SPARQLResult):
            for row in query_results:
                s_label = row[0].title()
                o_label = row[1].title()

                if s_label not in d:
                    d[s_label] = set()
                d[s_label].add(o_label)

        _update_dict(self.graph.query("""
            SELECT 
                ?s_label
                ?o_label
            WHERE {
                ?s cendant:implies ?o .
                ?s rdfs:label ?s_label .
                ?o rdfs:label ?o_label .
            }
        """))

        _results = {}
        for k in d:
            _results[k] = sorted(d[k])
        return _results

    def process(self) -> dict:
        return self._ontology_query()
