# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from graphviz import Digraph

from base import BaseObject


class DigraphNodeGenerator(BaseObject):
    """
    Purpose:
    Node Generation for a graphviz.Digraph object

    Notes:
    -   'digraph-node-generator' is not the same as 'graphviz-node-generator'
    -   this module generates a library-specific Digraph node element
    -   the graphviz-node-generator creates a string value that conforms to the Graphviz format

    Traceability:
    https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1426#issuecomment-16165027
    """

    def __init__(self,
                 graph_style: dict,
                 is_debug: bool = True):
        """
        Created:
            21-Nov-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1426#issuecomment-16165027
        :param graph_style:
            a graph style defined in a graph stylesheet
            e.g.:
            -   resources/config/graph/graphviz_nlp_graph.yml
            -   resources/config/graph/graphviz_big_graph.yml
        :param is_debug:
            True     increase log output at DEBUG level
        """
        BaseObject.__init__(self, __name__)
        from datagraph.graphviz.dmo import DigraphTextCleanser
        from datagraph.graphviz.dmo import NodeStyleMatcher

        self.is_debug = is_debug
        self._node_style_matcher = NodeStyleMatcher(is_debug=is_debug,
                                                    graph_style=graph_style)

        self._text_cleanser = DigraphTextCleanser(is_debug=self.is_debug,
                                                  graph_style=graph_style)

    def process(self,
                graph: Digraph,
                a_node_name: str,
                a_node_type: str,
                is_primary: bool,
                is_variant: bool) -> Digraph:
        """
        :param graph:
        :param a_node_name:
        :param a_node_type:
        :param is_primary:
        :param is_variant:
        :return:
        """

        d_node_style = self._node_style_matcher.process(a_tag_type=a_node_type,
                                                        is_variant=is_variant,
                                                        is_primary=is_primary)

        a_node_name = self._text_cleanser.process(a_node_name)
        graph.node(a_node_name,
                   label=a_node_name,
                   **d_node_style)

        return graph
