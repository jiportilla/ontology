# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from graphviz import Digraph
from pandas import DataFrame

from base import BaseObject


class GenerateInferenceGraph(BaseObject):
    """
    Purpose:
    Service that creates a Graphviz diagram exploring a pandas DataFrame of Cendant Ontology relationships

    Traceability:
    https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/213

    Prereq:
    OntologyAPI.relationships(...) -> DataFrame
    """

    def __init__(self,
                 df: DataFrame,
                 graph_style: str = "nlp",
                 is_debug: bool = True):
        """
        Created:
            7-May-2019
            craig.trim@ibm.com
        Updated:
            10-May-2019
            craig.trim@ibm.com
            *   add graphviz styling
            *   renamed from 'generate-graphviz-rels'
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/240
            *   moved from 'nlusvc' into datagraph:graphviz
        Updated:
            8-Oct-2019
            craig.trim@ibm.com
            *   update UID generation methodology
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1048
        :param df:
            the dataframe containing:
            ExplicitSchema 	    ExplicitTag 	            ImplicitSchema 	    ImplicitTag 	        Relationship
            soft skill 	        Communication Skill 	    soft skill 	        Presentation Skill 	    SimilarTo
            soft skill 	        Presentation Skill 	        soft skill 	        Communication Skill 	SimilarTo
            data science 	    Artificial Intelligence 	data science 	    Deep Learning 	        SimilarTo
            data science 	    Deep Learning 	            data science 	    Artificial Intelligence SimilarTo
            data science 	    Deep Learning 	            data science 	    Machine Learning 	    SimilarTo
        :param graph_style:
            the name of the graph stylesheet to use
        :param is_debug:
            if True     increase log output at DEBUG level
        """
        BaseObject.__init__(self, __name__)
        from datagraph.graphviz.dmo import GraphStyleLoader
        from datagraph.graphviz.dmo import DigraphTextCleanser
        from datagraph.graphviz.dmo import DigraphEdgeGenerator
        from datagraph.graphviz.dmo import DigraphNodeGenerator
        from datagraph.graphviz.dmo import NodeStyleMatcher

        self.df = df
        self.s_unique = set()
        self.is_debug = is_debug

        self._style_loader = GraphStyleLoader(style_name=graph_style,
                                              is_debug=self.is_debug)

        self._node_style_matcher = NodeStyleMatcher(is_debug=self.is_debug,
                                                    graph_style=self._style_loader.style())

        self._node_generator = DigraphNodeGenerator(is_debug=self.is_debug,
                                                    graph_style=self._style_loader.style())

        self._edge_generator = DigraphEdgeGenerator(is_debug=self.is_debug,
                                                    graph_style=self._style_loader.style())

        self._cleanse_text = DigraphTextCleanser(is_debug=self.is_debug,
                                                 graph_style=self._style_loader.style())

    def _add_nodes(self,
                   graph: Digraph) -> None:
        def _schema_elements() -> list:
            s = set()
            [s.add(x) for x in self.df.ExplicitSchema.unique()]
            [s.add(x) for x in self.df.ImplicitSchema.unique()]
            return sorted(s)

        for schema in _schema_elements():
            graph = self._node_generator.process(graph,
                                                 a_node_name=schema,
                                                 a_node_type="schema",
                                                 is_primary=True,
                                                 is_variant=False)

        for _, row in self.df.iterrows():
            graph = self._node_generator.process(graph,
                                                 a_node_name=row["ExplicitTag"],
                                                 a_node_type="tag",
                                                 is_primary=True,
                                                 is_variant=False)

            graph = self._node_generator.process(graph,
                                                 a_node_name=row["ImplicitTag"],
                                                 a_node_type="tag",
                                                 is_primary=False,
                                                 is_variant=row["Relationship"] == "Variant")

    def _add_edges(self,
                   graph: Digraph) -> None:
        for _, row in self.df.iterrows():
            graph = self._edge_generator.process(graph,
                                                 row["ExplicitTag"],
                                                 "type-of",
                                                 row["ExplicitSchema"])

            graph = self._edge_generator.process(graph,
                                                 row["ImplicitTag"],
                                                 "type-of",
                                                 row["ImplicitSchema"])

            graph = self._edge_generator.process(graph,
                                                 row["ExplicitTag"],
                                                 row["Relationship"],
                                                 row["ImplicitTag"])

    def process(self,
                engine: str = "fdp",
                file_extension: str = "png",
                file_name: str = "inference") -> Digraph:
        """
        :param engine:
            'dot', 'neato', 'twopi', 'circo', 'fdp', 'sfdp', 'patchwork', 'osage',
        :param file_extension:
            'png'
        :param file_name:
            the filename
        :return:
            a Digraph object
        """

        graph = Digraph(engine=engine,
                        comment='Schema',
                        format=file_extension,
                        name=file_name)

        graph.attr('node',
                   **self._node_style_matcher.default_node_style())

        self._add_nodes(graph)
        self._add_edges(graph)

        return graph
