# !/usr/bin/env python
# -*- coding: UTF-8 -*-


import networkx as nx
import pandas as pd
from networkx import Graph
from pandas import DataFrame
from pandas import Series

from base import BaseObject
from datadict import FindDimensions
from datadict import FindSynonym


class GenerateSchemaNetworkX(BaseObject):
    """
    Purpose:
    Service that creates a NetworkX diagram explaining the relationships/inferences for a dimensionality schema

    Traceability:

    Prereq:

    """

    def __init__(self,
                 some_input,
                 ontology_name: str = 'base',
                 is_debug: bool = True):
        """
        Created:
            29-Apr-2019
            craig.trim@ibm.com
            *   copied from 'generate-query-networkx'
        Updated:
            15-Oct-2019
            craig.trim@ibm.com
            *   added static method for 'by_schema'
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1116#issuecomment-15314633
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
        :param some_input:
            the dataframe or series containing the tokens and their variations
        :param is_debug:
            if True     increase log output at DEBUG level
        """
        BaseObject.__init__(self, __name__)

        self._input = some_input
        self._is_debug = is_debug
        self._syn_finder = FindSynonym(is_debug=is_debug,
                                       ontology_name=ontology_name)

    @staticmethod
    def _nodes_from_df(some_df: DataFrame) -> list:
        """
        :return:
            a unique and sorted list of nodes for the diagram
        """

        n = set()

        n.add('root')
        [n.add(x) for x in some_df.Child.unique()]
        [n.add(x) for x in some_df.Parent.unique()]

        return sorted(n)

    @staticmethod
    def _rels_from_df(some_df: DataFrame) -> list:
        """
        Sample Dataframe:
                Child 	        Parent
            0 	servicenow 	    service now
            1 	agile 	        agile
            2 	big data 	    big data
            3 	spark 	        big data
            4 	hadoop 	        big data
            5 	python 	        data science
            6 	data scientist 	data science
            7 	data analyst 	data science
        :return:
            a sorted list of tuples with relationships (subject, object, predicates)
        """
        n = set()

        for parent in some_df.Parent.unique():
            n.add(("root",
                   parent,
                   "child"))

        for _, row in some_df.iterrows():
            n.add((row["Parent"],
                   row["Child"],
                   "part-of"))

        return sorted(n)

    @staticmethod
    def _nodes_from_series(some_series: Series) -> list:
        """
        :return:
            a unique and sorted list of nodes for the diagram
        """

        n = set()

        n.add('root')
        [n.add(x) for x in list(some_series)]

        return sorted(n)

    @staticmethod
    def _rels_from_series(some_series: Series) -> list:
        """
        :return:
            a sorted list of tuples with relationships (subject, object, predicates)
        """
        n = set()

        for parent in list(some_series):
            n.add(("root",
                   parent,
                   "child"))

        return sorted(n)

    @staticmethod
    def by_schema(xdm_schema: str):
        """
        :param xdm_schema:
            the name of the schema to perform the type lookup
            Notes:
            -   typically either 'supply' or 'learning'
            -   the full list is on path 'resources/config/dimensionality'
        :return:
        """
        records = []

        def _struct(a_k, a_v) -> dict:
            return {"Parent": a_k, "Child": a_v}

        finder = FindDimensions(xdm_schema)
        for k in finder.top_level_entities():
            print(k)
            for v in finder.children(k):
                records.append(_struct(k, v))

        return pd.DataFrame(records)

    def process(self) -> Graph:
        g = nx.Graph()

        if type(self._input) == DataFrame:
            [g.add_node(node) for node in self._nodes_from_df(self._input)]

            for rel in self._rels_from_df(self._input):
                g.add_edge(rel[0],
                           rel[1])

            d_edge_labels = {}
            for rel in self._rels_from_df(self._input):
                d_edge_labels[(rel[0], rel[1])] = rel[2]

        elif type(self._input) == Series:
            [g.add_node(node) for node in self._nodes_from_series(self._input)]

            for rel in self._rels_from_series(self._input):
                g.add_edge(rel[0],
                           rel[1])

            d_edge_labels = {}
            for rel in self._rels_from_series(self._input):
                d_edge_labels[(rel[0], rel[1])] = rel[2]

        else:
            raise NotImplementedError(f"Unrecognized Input Type ("
                                      f"type={type(self._input)})")

        return g
