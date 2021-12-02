# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject


class GenerateGraphvizFile(BaseObject):
    """
    Purpose:
    Service that creates a Graphviz diagram exploring a pandas DataFrame of Cendant Ontology relationships

    Traceability:
    https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1094

    Sample Input:
        [   "a -> b",
            "b -> c",
            "a -> c"    ]

    Sample Ouptut:
        digraph CendantGraph {

            /* NODE DEFINITIONS */
            a [label="a" size="150,150" shape="oval" colorscheme="bugn9" color=2 style="filled"];
            b [label="b" size="150,150" shape="oval" colorscheme="bugn9" color=2 style="filled"];
            c [label="c" size="150,150" shape="oval" colorscheme="bugn9" color=2 style="filled"];

            /* EDGE DEFINITIONS */
            a -> b [penwidth=0.33 weight=2];
            b -> c [penwidth=0.33 weight=2];
            a -> c [penwidth=0.33 weight=2];
        }

    Prereq:
    None
    """

    _graphviz = []

    def __init__(self,
                 rels: list,
                 node_size: int = 150,
                 node_shape: str = "oval",
                 node_style: str = "filled",
                 color_scheme: str = "bugn9",
                 is_debug: bool = True):
        """
        Created:
            11-Oct-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1094
        :param rels:
            a list of relationships
            Sample Input:
                [   "a -> b",
                    "b -> c",
                    "a -> c"    ]
            or
                [   "a -- b",
                    "b -- c",
                    "a -- c"    ]
        :param node_size:
            the size of the node
            this value will be repeated (e.g, '150,150')
        :param node_shape:
            the shape of the node
            https://www.graphviz.org/doc/info/shapes.html
        :param node_style:
            the node style
            acceptable values: filled, invisible, diagonals, rounded. dashed, dotted, solid and bold
            https://www.graphviz.org/doc/info/shapes.html
        :param color_scheme:
            the graphviz color scheme
            https://graphviz.org/doc/info/colors.html
        :param is_debug:
            if True     increase log output at DEBUG level
        """
        BaseObject.__init__(self, __name__)

        self._rels = rels
        self._is_debug = is_debug

        self._node_size = node_size
        self._node_style = node_style
        self._node_shape = node_shape
        self._color_scheme = color_scheme

        if self._is_debug:
            self.logger.debug(f"Instantiated GenerateGraphvizFile "
                              f"(total-rels={len(self._rels)})")

    def _parse(self) -> list:
        """
        :return:
            parse incoming data
        """
        triples = []

        def _triple(s: str,
                    p: str,
                    o: str) -> dict:
            return {"s": s, "p": p, "o": o}

        for rel in self._rels:
            if '--' in rel:
                tokens = [x.strip() for x in rel.split('--')]
                triples.append(_triple(s=tokens[0], p='--', o=tokens[1]))
            elif '->' in rel:
                tokens = [x.strip() for x in rel.split('->')]
                triples.append(_triple(s=tokens[0], p='->', o=tokens[1]))
            else:
                raise ValueError(f"Unrecognized Relationship: {rel}")

        return triples

    def _graph_type(self) -> str:
        """
        Purpose:
            Perform edge-type validation
            Graphviz only supports directed-only or undirected-only
        :return:
            a graph type
        """

        undirected = 0
        directed = 0

        for rel in self._rels:
            if '->' in rel:
                directed += 1
            elif '--' in rel:
                undirected += 1
            if directed > 0 and undirected > 0:
                raise ValueError("Directed and Undirected Edges Detected")

        if directed > 0:
            return "digraph"
        if undirected > 0:
            return "graph"

        raise NotImplementedError("No Edge Types Detected")

    def _generate_graph(self,
                        d_node_styles: dict,
                        d_edge_styles: dict) -> None:
        graph_type = self._graph_type()

        self._graphviz.append(graph_type + " CendantGraph {")

        self._graphviz.append("\t/* NODE DEFINITIONS */")
        for node_id in d_node_styles:
            self._graphviz.append(f"\t{node_id} {d_node_styles[node_id]}")

        self._graphviz.append("\n")

        self._graphviz.append("\t/* EDGE DEFINITIONS */")
        for edge_id in d_edge_styles:
            self._graphviz.append(f"\t{edge_id} {d_edge_styles[edge_id]}")

        self._graphviz.append("}")

    def process(self) -> list:
        """
        :return:
            a styled graphviz graph
        """
        from datagraph.graphviz.dmo import GraphvizEdgeGenerator
        from datagraph.graphviz.dmo import GraphvizNodeGenerator

        d_edge_styles = GraphvizEdgeGenerator(rels=self._rels,
                                              is_debug=self._is_debug).process()

        d_node_styles = GraphvizNodeGenerator(rels=self._rels,
                                              node_size=self._node_size,
                                              node_shape=self._node_shape,
                                              node_style=self._node_style,
                                              color_scheme=self._color_scheme,
                                              is_debug=self._is_debug).process()

        self._generate_graph(d_node_styles=d_node_styles,
                             d_edge_styles=d_edge_styles)
        return self._graphviz
