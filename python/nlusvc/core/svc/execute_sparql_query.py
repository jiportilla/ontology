#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from nltk import stem
from rdflib import Graph
from rdflib.plugins.sparql.processor import SPARQLResult
from rdflib.query import ResultRow

from base import BaseObject
from datadict import FindEntity
from datagraph import OwlGraphConnector


class ExecuteSparqlQuery(BaseObject):
    """ Execute a Sparql Query against the Cendant Ontology
    """

    stemmer = stem.PorterStemmer()

    def __init__(self,
                 sparql_query: str,
                 ontology_name: str = 'base',
                 is_debug: bool = False):
        """
        Created:
            26-Jul-2019
            craig.trim@ibm.com
        :param sparql_query:
            any valid SPARQL query
        :param ontology_name:
            any valid RDF Graph name (e.g., 'cendant')
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._entity_finder = FindEntity()
        self._graph = self._load(ontology_name)
        self._results = self._query(sparql_query)

    def _load(self,
              graph_name: str) -> Graph:
        """
        Purpose:
            Load an RDF Graph by name
        :param graph_name:
            any valid RDF Graph name (e.g., 'cendant')
        :return:
            an in-memory rdflib.Graph object
        """
        return OwlGraphConnector(is_debug=self._is_debug,
                                 ontology_name=graph_name).process()

    def _query(self,
               sparql_query: str) -> SPARQLResult:
        """
        Purpose:
            execute a SPARQL query against an RDF Graph
        :param sparql_query:
            any valid SPARQL query
        :return:
            a SPARQLResult object
            iteration of this object is simple:
                for rdflib.query.ResultRow in rdflib.plugins.sparql.processor.SPARQLResult:
                    ... result_row[0]
                    ... result_row[1]
                    etc

            the rdflib.query.ResultRow is a list and each SPARQL query variable
            is a position in that list

            for example:
                SELECT ?s ?o WHERE { ?s cendant:implies ?o }

            Result Iteration:
                ResultRow[0] = ?s
                ResultRow[1] = ?o
        """
        return self._graph.query(sparql_query)

    def results(self) -> SPARQLResult:
        return self._results

    def size(self) -> int:
        return len(self.results())

    def label(self,
              row: ResultRow,
              at_position: int) -> str:
        """
        Purpose:
            for a given ResultRow and a given position, return a label result
        :param row:
            any rdflib.query.ResultRow
        :param at_position:
            the variable position
            for example:
                SELECT ?s ?o WHERE { ?s cendant:implies ?o }
            the ResultRow object has two positions:
                ResultRow[0] = ?s
                ResultRow[1] = ?o
        :return:
            a properly formatted label
        """

        def _label():
            _row_text = row[at_position]
            if "http" in _row_text:
                return str(_row_text).split('#')[-1].strip().replace('_', ' ')
            return _row_text.title()

        row_label = _label()
        if not row_label or not len(row_label):
            raise ValueError(f"No Label Found (row={row}, pos={at_position})")

        return self._entity_finder.label_or_self(row_label)
