#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from rdflib import Graph
from rdflib import Literal
from rdflib.plugins.sparql.processor import SPARQLResult

from base import BaseObject
from base import MandatoryParamError


class OntologyLabelGenerator(BaseObject):
    def __init__(self,
                 some_graph: Graph):
        """
        Created:
            27-Feb-2019
            craig.trim@ibm.com
            *   refactored out of generate-ontology-dict
        Updated:
            1-Apr-2019
            craig.trim@ibm.com
            *   added 'see-also-by-label' and created generic methods
            *   added '_extract_label_text'
        Updated:
            16-Jul-2019
            craig.trim@ibm.com
            *   added 'see-also' method
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/444
        """
        BaseObject.__init__(self, __name__)
        if not some_graph:
            raise MandatoryParamError("Ontology Graph")

        self.graph = some_graph

    @staticmethod
    def _extract_label_text(some_label):
        """
        Updated:
            1-Apr-2019
            craig.trim@ibm.com
            *   learned this the hard way:
                the use of
                    <rdflib.Literal>.title()
                to extract the label text will transform a string like
                    'RedHat Linux'
                to
                    'Redhat Linux'
                go figure ...
        :param some_label:
        :return:
            label text
        """

        if type(some_label) == Literal:
            return str(some_label).split("(")[-1].split(")")[0]
        if type(some_label) == str:
            return some_label

        raise ValueError("\n".join([
            "Unrecognized Label Type",
            "\tactual-type: {}".format(type(some_label))
        ]))

    def _sparql_query(self,
                      some_query: str) -> dict:
        d = {}

        def _update_dict(query_results: SPARQLResult):
            for row in query_results:
                label = self._extract_label_text(row[0])
                entity = row[1].title()
                d[label] = entity

        _update_dict(self.graph.query(some_query))

        return d

    def labels(self) -> dict:
        return self._sparql_query("""
            SELECT
                ?label ?entity
            WHERE {
                ?entity rdfs:label ?label .
            }
        """)

    def see_also(self) -> dict:
        return self._sparql_query("""
            SELECT
                ?label ?entity
            WHERE {
                ?entity rdfs:seeAlso ?label .
            }
        """)

    def see_also_by_label(self,
                          some_label: str) -> list:
        q = """
            SELECT
                ?seeAlso
            WHERE {
                ?entity rdfs:label '#label' .
                ?entity rdfs:seeAlso ?seeAlso .
            }
        """.replace("#label", some_label)

        values = set()
        for row in self.graph.query(q):
            [values.add(x.strip()) for x in self._extract_label_text(row[0]).split(",")]
        return sorted(values)

    def infinitive_by_label(self,
                            some_label: str) -> list:
        q = """
            SELECT
                ?infinitive
            WHERE {
                ?entity rdfs:label '#label' .
                ?entity cendant:infinitive ?infinitive .
            }
        """.replace("#label", some_label)

        values = set()
        for row in self.graph.query(q):
            [values.add(x.strip()) for x in self._extract_label_text(row[0]).split(",")]
        return sorted(values)
