# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from graphviz import Digraph

from base import BaseObject


class DigraphEdgeGenerator(BaseObject):
    """
    Purpose:
    Edge Generation for a graphviz.Digraph object

    Notes:
    -   'digraph-edge-generator' is not the same as 'graphviz-edge-generator'
    -   this module generates a library-specific Digraph edge element
    -   the graphviz-edge-generator creates a string value that conforms to the Graphviz format

    Traceability:
    https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1426#issuecomment-16165027
    """

    __s_unique = set()

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
        from datagraph.graphviz.dmo import EdgeStyleMatcher

        self.is_debug = is_debug
        self._edge_style_matcher = EdgeStyleMatcher(is_debug=self.is_debug,
                                                    graph_style=graph_style)
        self._text_cleanser = DigraphTextCleanser(graph_style=graph_style,
                                                  is_debug=self.is_debug)

    def process(self,
                graph: Digraph,
                a_subject: str,
                a_predicate: str,
                a_object: str) -> Digraph:

        if not a_subject or not a_predicate or not object:
            return graph

        uid = " ".join(sorted([a_subject.lower(), a_object.lower()]))

        def _is_valid():
            if "unlisted" in uid:
                return False
            return uid not in self.__s_unique and a_subject != a_object

        if _is_valid():
            self.__s_unique.add(uid)
            d_edge = self._edge_style_matcher.process(a_subject=a_subject,
                                                      a_predicate=a_predicate,
                                                      a_object=a_object)

            if "display_label" in d_edge:
                if not d_edge["display_label"]:
                    a_predicate = ''

            graph.edge(tail_name=self._text_cleanser.process(a_subject),
                       head_name=self._text_cleanser.process(a_object),
                       label=self._text_cleanser.process(a_predicate),
                       **d_edge["style"])

        return graph
