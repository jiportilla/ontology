#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from rdflib import Graph
from rdflib.plugins.sparql.processor import SPARQLResult

from base import BaseObject
from base import MandatoryParamError


class OntologyParentGenerator(BaseObject):
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
                child_label = row[0].title()
                parent_label = row[1].title()

                if child_label not in d:
                    d[child_label] = set()

                if parent_label.lower().strip() != child_label.lower().strip():
                    d[child_label].add(parent_label)

        _update_dict(self.graph.query("""
            SELECT
                ?child_label 
                ?parent_label
            WHERE {
                ?child rdfs:subClassOf ?parent .
                ?child rdfs:seeAlso ?child_label .
                ?parent rdfs:label ?parent_label .
            }
        """))

        _update_dict(self.graph.query("""
            SELECT
                ?child_label 
                ?parent_label
            WHERE {
                ?child rdfs:subClassOf ?parent .
                ?child rdfs:label ?child_label .
                ?parent rdfs:label ?parent_label .
            }
        """))

        _update_dict(self.graph.query("""
            SELECT
                ?child_label
                ?parent_label
            WHERE {
                ?child rdf:type+ ?parent . ?parent rdf:type+ owl:Class . 
                ?child rdfs:label ?child_label .
                ?parent rdfs:label ?parent_label .
            }
        """))

        _update_dict(self.graph.query("""
            SELECT
                ?child_label
                (str('root') as ?parent_label)
            WHERE {
                ?child  rdfs:label ?child_label ;
                        rdf:type owl:Class .
                FILTER NOT EXISTS { ?child rdfs:subClassOf ?x }                
            }
        """))

        _d = {}
        for k in d:
            _d[k] = sorted(d[k])
        return _d

    def process(self) -> dict:
        return self._ontology_query()
