# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from collections import Counter

from base import BaseObject


class GraphvizNodeGenerator(BaseObject):
    """
    Purpose:
    Node Generation for a Graphviz using plain text

    Notes:
    -   'graphviz-node-generator' is not the same as 'digraph-node-generator'
    -   this module creates a string value that conforms to the Graphviz format
    -   the graphviz-node-generator generates a library-specific Digraph node element

    Traceability:
    """

    _graphviz = []

    def __init__(self,
                 rels: list,
                 node_size: int,
                 node_shape: str,
                 node_style: str,
                 color_scheme: str,
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
        self._is_debug = is_debug

        self._rels = rels
        self._node_size = node_size
        self._node_style = node_style
        self._node_shape = node_shape
        self._color_scheme = color_scheme

    def _count_nodes(self) -> Counter:
        c = Counter()

        for rel in self._rels:
            if '--' in rel:
                [c.update({x.strip(): 1}) for x in rel.split('--')]
            elif '->' in rel:
                [c.update({x.strip(): 1}) for x in rel.split('->')]

        return c

    def _define_nodes(self,
                      c_nodes: Counter) -> dict:
        """
        Purpose:
            Style the Nodes
        Sample Input:
            (('a', 1), ('b', 1), ('c', 1))
        Sample Output:
            {   'a': '[label="a" size="150,150" shape="oval" colorscheme="bugn9" color=2 style="filled"]',
                'b': '[label="b" size="150,150" shape="oval" colorscheme="bugn9" color=2 style="filled"]',
                'c': '[label="c" size="150,150" shape="oval" colorscheme="bugn9" color=2 style="filled"]'}
        :param c_nodes:
            a counter of nodes
        :return:
            a dictionary of styled output
        """

        d_styles = {}

        def _color(freq: int) -> int:
            if freq >= 9:
                return 9
            if freq <= 1:
                return 1
            return freq

        for x in c_nodes.most_common():
            node_id = x[0].replace(' ', '_').lower()

            style = [
                f"label=\"{x[0]}\"",
                f"size=\"{self._node_size},{self._node_size}\"",
                f"shape=\"{self._node_shape}\"",
                f"colorscheme=\"{self._color_scheme}\"",
                f"color={_color(x[1])}",
                f"style=\"{self._node_style}\""]

            d_styles[node_id] = f"[{' '.join(style)}];"

        return d_styles

    def process(self):
        return self._define_nodes(self._count_nodes())
