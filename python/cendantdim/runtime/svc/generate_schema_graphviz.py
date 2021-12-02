# !/usr/bin/env python
# -*- coding: UTF-8 -*-

import pandas as pd
from graphviz import Digraph

from base import BaseObject


class GenerateSchemaGraphviz(BaseObject):
    """
    Purpose:
    Service that creates a Graphviz diagram explaining the parent/child of database schema

    Traceability:
    https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/183

    Prereq:

    """

    def __init__(self,
                 df: pd,
                 is_debug: bool = True):
        """
        Created:
            1-May-2019
            anassar@us.ibm.com
        :param df:
            the dataframe containing the parents and child schema
        :param is_debug:
            if True     increase log output at DEBUG level
        """
        BaseObject.__init__(self, __name__)

        self.df = df
        self.is_debug = is_debug

    def _parents_nodes(self) -> list:
        """
        :return:
            a unique and sorted list of parents nodes for the diagram
        """

        n = set()

        [n.add(x) for x in self.df.Parent.unique()]

        return sorted(n)

    def _children_nodes(self, parent) -> list:
        """
        :return:
            a unique and sorted list of parents nodes for the diagram
        """

        n = set()
        [n.add(x) for x in self.df[self.df.Parent == parent]["Child"].unique() if x != parent]
        return sorted(n)

    def process(self,
                file_extension: str = "svg",
                file_name: str = "schema") -> Digraph:
        # Step: Define the Graph
        f = Digraph(comment='Schema',
                    format=file_extension,
                    name=file_name)

        # Step: Add Nodes
        f.attr(size='150,150')
        f.attr('node', shape='oval',
               color='lightblue2', style='filled')

        f.node("root")
        parents_nodes = self._parents_nodes()
        [f.edge("root", node) for node in parents_nodes]

        f.attr('node',
               shape='circle', color='lightblue2', style='filled')
        # Step: Add Relationships
        for parent in parents_nodes:
            [f.edge(parent, child) for child in self._children_nodes(parent)]

        return f
