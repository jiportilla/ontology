#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from nlusvc import ExecuteSparqlQuery


class CertificationHierarchyGenerator(BaseObject):
    """ Generate a mapping file for parent-child Certification relationships """

    def __init__(self,
                 is_debug: bool = True):
        """
        Created:
            6-Aug-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/629
        Updated:
            15-Oct-2019
            xavier.verges@es.ibm.com
            *   sorted lists to make changes in the generated files easier to track
        """
        BaseObject.__init__(self, __name__)

        self._map = {}
        self._is_debug = True

    def _normalize(self):
        for k in self._map:
            self._map[k] = sorted(list(self._map[k]))

    def _execute_query(self,
                       sparql_query: str):
        esq = ExecuteSparqlQuery(sparql_query=sparql_query,
                                 is_debug=self._is_debug)

        for result in esq.results():
            parent_label = esq.label(result, 0).lower()
            child_label = esq.label(result, 1).lower()

            if parent_label not in self._map:
                self._map[parent_label] = set()
            self._map[parent_label].add(child_label)

    def _query1(self):
        self._execute_query("""
        SELECT
            ?parent_label
            ?child_label
        WHERE {
            ?parent rdfs:subClassOf+ cendant:Certification .
            ?child rdfs:subClassOf+ ?parent .
            
            ?parent rdfs:label ?parent_label .            
            ?child rdfs:label ?child_label .
        }""")

    def process(self) -> dict:
        self._query1()
        self._normalize()

        return self._map
