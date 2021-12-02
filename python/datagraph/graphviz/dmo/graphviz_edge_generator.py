# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from collections import Counter

from base import BaseObject


class GraphvizEdgeGenerator(BaseObject):
    """
    Purpose:
    Edge Generation for a Graphviz using plain text

    Notes:
    -   'graphviz-edge-generator' is not the same as 'digraph-edge-generator'
    -   this module creates a string value that conforms to the Graphviz format
    -   the graphviz-edge-generator generates a library-specific Digraph edge element

    Traceability:
    """

    _graphviz = []

    def __init__(self,
                 rels: list,
                 is_debug: bool = True):
        """
        Created:
            11-Oct-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1094
        :param is_debug:
            if True     increase log output at DEBUG level
        """
        BaseObject.__init__(self, __name__)

        self._rels = rels
        self._is_debug = is_debug

    def _count_edges(self) -> Counter:
        c = Counter()
        for rel in self._rels:
            c.update({rel: 1})

        return c

    @staticmethod
    def _define_edges(c_edges: Counter):
        d_styles = {}
        for edge in c_edges.most_common():
            def _penwidth():
                return round(edge[1] / 3, 2)

            def _weight():
                return edge[1] * 2

            style = [
                f"penwidth={_penwidth()}",
                f"weight={_weight()}"]

            edge_id = edge[0].replace(' ', '_').lower()
            edge_id = edge_id.replace('_--_', ' -- ')
            edge_id = edge_id.replace('_->_', ' -> ')

            d_styles[edge_id] = f"[{' '.join(style)}];"

        return d_styles

    def process(self) -> dict:
        return self._define_edges(self._count_edges())
