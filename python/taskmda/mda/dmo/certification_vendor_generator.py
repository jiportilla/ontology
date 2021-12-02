#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from nlusvc import ExecuteSparqlQuery


class CertificationVendorGenerator(BaseObject):
    """ Generate a mapping file that associates certifications to Vendors """

    def __init__(self,
                 is_debug: bool = True):
        """
        Created:
            5-Aug-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/606#issuecomment-13673863
        """
        BaseObject.__init__(self, __name__)

        self._map = {}
        self._is_debug = True

    def _execute_query(self,
                       sparql_query: str):
        esq = ExecuteSparqlQuery(sparql_query=sparql_query,
                                 is_debug=self._is_debug)

        for result in esq.results():
            cert_label = esq.label(result, 0)
            owner_label = esq.label(result, 1)

            self._map[cert_label] = owner_label

    def _query1(self):
        self._execute_query("""
        SELECT
            ?cert_label
            ?owner_label
        WHERE {

            ?cert rdfs:subClassOf+ cendant:Certification .    
            ?cert rdfs:subClassOf+ ?z .
            ?z cendant:ownedBy+ ?owner .
            ?cert rdfs:label ?cert_label .
            ?owner rdfs:label ?owner_label .
        }""")

    def _query2(self):
        self._execute_query("""
        SELECT
            ?cert_label
            ?owner_label
        WHERE {
            ?cert rdfs:subClassOf+ cendant:Certification .    
            ?cert cendant:ownedBy+ ?owner .
            ?cert rdfs:label ?cert_label .
            ?owner rdfs:label ?owner_label .
        }""")

    def process(self) -> dict:
        self._query1()
        self._query2()

        return self._map
