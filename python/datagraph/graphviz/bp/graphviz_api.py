# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from graphviz import Digraph
from pandas import DataFrame

from base import BaseObject


class GraphvizAPI(BaseObject):
    """ Purpose: Graphviz API one-stop shop
    """

    def __init__(self,
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

    def synonyms(self,
                 synonyms: list,
                 node_size: int = 150,
                 node_shape: str = "oval",
                 node_style: str = "filled",
                 color_scheme: str = "bugn9") -> list:
        """
        :param synonyms:
            a list of canonical terms from the syonyms_kb.csv file
            Sample Input:
                [ 'Public Blockchain' ]
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
        :return:
        """
        from datagraph.graphviz.svc import GenerateSynonymsGraph
        rels = GenerateSynonymsGraph(synonyms=synonyms,
                                     is_debug=self._is_debug).process()

        return self.generate_file(rels=rels,
                                  node_size=node_size,
                                  node_shape=node_shape,
                                  node_style=node_style,
                                  color_scheme=color_scheme)

    def inference_graph(self,
                        df: DataFrame,
                        graph_style: str = 'big',
                        engine: str = 'fdp') -> Digraph:
        """
        :param df:
            OntologyAPI.inference(...)
            Sample Input:
                +----+------------------+---------------+------------------+--------------------------+-------------+-------------+----------------+
                |    | ExplicitSchema   | ExplicitTag   | ImplicitSchema   | ImplicitTag              | IsPrimary   | IsVariant   | Relationship   |
                |----+------------------+---------------+------------------+--------------------------+-------------+-------------+----------------|
                |  0 | cloud            | AWS           | cloud            | Public Cloud             | False       | False       | Implies        |
                |  1 | cloud            | AWS           | hard skill       | Technical Services       | False       | False       | Implies        |
                |  2 | cloud            | AWS           | hard skill       | Amazon Machine Image     | False       | False       | PartOf         |
                |  3 | cloud            | AWS           | cloud            | EC2                      | False       | False       | PartOf         |
                |  4 | cloud            | AWS           | other            | Amazon                   | False       | False       | OwnedBy        |
                |  5 | cloud            | AWS           | cloud            | Cloud Computing Platform | False       | False       | Parent         |
                |  6 | cloud            | AWS           | cloud            | Cloud Computing Platform | False       | False       | References     |
                |  7 | cloud            | AWS           | cloud            | EC2                      | False       | False       | References     |
                +----+------------------+---------------+------------------+--------------------------+-------------+-------------+----------------+
        :param graph_style:
            'nlp', 'big'
        :param engine:
            all these options are supported:
                'dot', 'neato', 'twopi', 'circo', 'fdp', 'sfdp', 'patchwork', 'osage'
            these options are recommended:
                'dot'   creates a hierarchy, suitable for smaller graphs
                        where parent/child detail is important
                'fdp'   creates a clustered graph
                        where 'whole graph' patterns are essential
        :return:
            a Graphviz digrrah
        """
        from datagraph import GenerateInferenceGraph
        return GenerateInferenceGraph(df=df,
                                      is_debug=self._is_debug,
                                      graph_style=graph_style).process(engine=engine)

    def generate_file(self,
                      rels: list,
                      node_size: int = 150,
                      node_shape: str = "oval",
                      node_style: str = "filled",
                      color_scheme: str = "bugn9") -> list:
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
        :return:
            parse incoming data
        """
        from datagraph.graphviz.svc import GenerateGraphvizFile
        return GenerateGraphvizFile(rels=rels,
                                    node_size=node_size,
                                    node_shape=node_shape,
                                    node_style=node_style,
                                    color_scheme=color_scheme,
                                    is_debug=self._is_debug).process()

