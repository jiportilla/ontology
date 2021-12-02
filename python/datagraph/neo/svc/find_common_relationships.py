#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pandas as pd
from pyparsing import ParseException
from rdflib import Graph
from rdflib import Literal
from rdflib.query import ResultRow

from base import BaseObject
from base import LabelFormatter
from datadict import FindDimensions
from datadict import FindEntity


class FindCommonRelationships(BaseObject):
    """ Query the Cendant Ontology

        given
            alpha implies gamma
            beta implies gamma
        thus
            alpha <-> beta

        more generically
            ?a ?p ?z
            ?b ?p ?z
        where   ?p != rdf:type and
                ?p != rdfs:subClassOf

    """

    _s_terms = set()
    _d_results = {}

    def __init__(self,
                 graph: Graph = None,
                 xdm_schema: str = 'supply',
                 ontology_name: str = 'base',
                 is_debug: bool = False):
        """
        Created:
            18-Jul-2019
            craig.trim@ibm.com
            *   refactored out of generate-dataframe-rels
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/455#issuecomment-12884783
        Updated:
            22-Jul-2019
            craig.trim@ibm.com
            *   deprecated
                this runs to slow -- it's hard to control SPARQL query algebra in the wild
                I have added additional properties to 'find-relationships'
        Updated:
            29-Oct-2019
            craig.trim@ibm.com
            *   remove 'entity-schema-finder' in favor of new approach
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/916#issuecomment-15620801
        Updated:
            13-Dec-2019
            craig.trim@ibm.com
            *   load dictionaries by ontology name
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1582
        :param graph:
        :param xdm_schema:
            the name of the schema to perform the type lookup
            Notes:
            -   typically either 'supply' or 'learning'
            -   the full list is on path 'resources/config/dimensionality'
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)
        from datagraph.neo.dmo import OwlGraphConnector
        if not graph:
            graph = OwlGraphConnector(is_debug=is_debug,
                                      ontology_name="cendant").process()

        self._graph = graph
        self._is_debug = is_debug

        self._entity_finder = FindEntity(is_debug=is_debug,
                                         ontology_name=ontology_name)
        self._dim_finder = FindDimensions(schema=xdm_schema,
                                          is_debug=is_debug,
                                          ontology_name=ontology_name)

    @staticmethod
    def _query(term: str) -> str:
        return """
                    SELECT
                        ?a_label ?p
                    WHERE {
                        {
                            cendant:#1 ?p ?c .
                            ?a rdfs:label ?a_label .
                            ?a ?p ?c .
                        } 
                        UNION
                        {
                            ?z rdfs:subClassOf cendant:#1 . 
                            ?z ?p ?c .
                            ?a rdfs:label ?a_label .
                            ?a ?p ?c .
                        } 
                        UNION
                        {
                            cendant:#1 ?p ?c .
                            ?a rdfs:subClassOf ?c .
                            ?a rdfs:label ?a_label .
                        }
                    }
                """.strip().replace("#1", term)

    def _normalize_term(self,
                        term: str) -> str:
        original_term = term

        term = term.lower().replace("!", "")
        term = term.lower().replace("/", "_")
        term = term.lower().replace("_", " ")

        term = "_".join([LabelFormatter.camel_case(token)
                         for token in term.split(" ")])

        self.logger.debug("\n".join([
            f"Term Manipulation (original={original_term}, normalized={term}"]))

        return term

    def _related_terms(self,
                       term: str) -> list:
        term = self._normalize_term(term)

        def _to_result_set():
            try:
                sparql_query = self._query(term)
                return self._graph.query(sparql_query)
            except ParseException as e:
                self.logger.error("\n".join([
                    "Query Parse Exception",
                    f"\tTerm: {term}"]))

        result_set = _to_result_set()

        def _literal_extraction(a_label: Literal):
            if "http" in a_label:
                return str(a_label).split("'")[-1].split("'")[0].strip()
            return a_label.title()

        def _valid_href(a_row: ResultRow) -> bool:
            p = str(a_row[1].title()).lower()
            if "type" in p or "subclassof" in p or "owl" in p or "rdf-schema#" in p:
                return False
            # if "ownedby" in p:
            #     return False
            return True

        def _results() -> list:
            s = set()

            rows = [row for row in result_set if _valid_href(row)]
            for row in rows:

                literals = [x for x in row
                            if x and type(x) == Literal]
                for literal in literals:
                    s.add(_literal_extraction(literal))

                    _predicate = row[1]
                    _object = row[0]
                    self.logger.debug("\n".join([
                        "Located Relationship",
                        f"\tSubject: {term}",
                        f"\tPredicate: {_predicate}",
                        f"\tObject: {_object}"]))

            return sorted(s)

        results = _results()
        results = [result for result in results
                   if result.lower().replace("_", " ") !=
                   term.lower().replace("_", " ")]

        self.logger.debug("\n".join([
            "Find Related Terms Complete",
            f"\tOriginal Term: {term}",
            f"\tResults: {results}"]))

        self._d_results[term] = results
        return results

    def process(self,
                term: str):
        for related_term in self._related_terms(term):
            if related_term not in self._s_terms:
                self._s_terms.add(related_term)
                self._related_terms(related_term)

        results = []
        for key in self._d_results:
            for explicit_schema in self._dim_finder.find(key):

                for value in self._d_results[key]:
                    for implicit_schema in self._dim_finder.find(value):
                        results.append({
                            "ExplicitSchema": explicit_schema,
                            "ExplicitTag": key,
                            "ImplicitSchema": implicit_schema,
                            "ImplicitTag": value,
                            "IsPrimary": True,
                            "IsVariant": False,
                            "Relationship": "Implication"})

        df = pd.DataFrame(results)

        return df
