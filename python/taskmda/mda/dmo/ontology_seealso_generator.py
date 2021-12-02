#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from rdflib import Graph
from rdflib.plugins.sparql.processor import SPARQLResult

from base import BaseObject
from base import MandatoryParamError


class OntologySeeAlsoGenerator(BaseObject):
    def __init__(self,
                 some_graph: Graph):
        """
        Created:
            26-Mar-2019
            craig.trim@ibm.com
            *   based on 'ontology-label-generator'
        Updated:
            21-Nov-2019
            craig.trim@ibm.com
            *   allow multiple values per key
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1195#issuecomment-16167509
        """
        BaseObject.__init__(self, __name__)
        if not some_graph:
            raise MandatoryParamError("Ontology Graph")

        self.graph = some_graph

    def _ontology_query(self) -> dict:
        d = {}

        def _update_dict(query_results: SPARQLResult):
            for row in query_results:
                label = row[0].title()
                entity = row[1].title()

                if label not in d:
                    d[label] = set()

                if ',' in entity:
                    [d[label].add(x.strip()) for x in entity.split(',')]
                else:
                    d[label].add(entity)

        _update_dict(self.graph.query("""
            SELECT
                ?a_label
                ?b  
            WHERE {
                ?a rdfs:seeAlso ?b .
                ?a rdfs:label ?a_label .
            }
        """))

        return d

    def process(self) -> dict:
        d_seealso = self._ontology_query()

        d_normalized = {}
        for k in d_seealso:
            d_normalized[k] = sorted(d_seealso[k])

        return d_normalized
