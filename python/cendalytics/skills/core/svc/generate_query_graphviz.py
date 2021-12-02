# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from graphviz import Digraph
from pandas import DataFrame

from base import BaseObject
from datadict import FindSynonym


class GenerateQueryGraphviz(BaseObject):
    """
    Purpose:
    Service that creates a Graphviz diagram explaining the relationships/inferences
    for a single token and how this impacts an E/S query strategy

    Traceability:
    https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/141

    Prereq:
    an ElasticSearch index of the text must exist
    """

    def __init__(self,
                 df: DataFrame,
                 ontology_name:str='base',
                 is_debug: bool = True):
        """
        Created:
            24-Apr-2019
            craig.trim@ibm.com
        Updated:
            13-Dec-2019
            craig.trim@ibm.com
            *   load dictionaries by ontology name
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1582
        :param df:
            the dataframe containing the tokens and their variations
        :param is_debug:
            if True     increase log output at DEBUG level
        """
        BaseObject.__init__(self, __name__)

        self.df = df
        self.is_debug = is_debug
        self.synonym_finder = FindSynonym(is_debug=is_debug,
                                          ontology_name=ontology_name)

    def _nodes(self) -> list:
        """
        :return:
            a unique and sorted list of nodes for the diagram
        """

        n = set()

        [n.add(x) for x in self.df.Token.unique()]

        return sorted(n)

    def _rels(self) -> list:
        """
        Sample Dataframe:
                               Owner Relationship                  Token             Type
            0                   None         None                    AWS     PrimaryToken
            1                    AWS    variation           amazon cloud   PrimaryVariant
            2                    AWS    variation     amazon web_service   PrimaryVariant
            3                    AWS    variation                  atg10   PrimaryVariant
            4                    AWS    partonomy   Amazon Machine Image    InferredToken
            5                    AWS    partonomy                    EC2    InferredToken
            6                    AWS    ownership                 Amazon    InferredToken
            7   Amazon Machine Image    variation                    ami  InferredVariant
            8                    EC2    variation             amazon ec2  InferredVariant
            9                    EC2    variation        elastic compute  InferredVariant
            10                   EC2    variation  elastic compute cloud  InferredVariant
            11                Amazon    variation              amazoncom  InferredVariant
            12                Amazon    variation                amazons  InferredVariant
            13                Amazon    variation                  amzon  InferredVariant
            14                Amazon    variation                 amzons  InferredVariant
        :return:
            a sorted list of tuples with relationships (subject, object, predicates)
        """
        n = set()

        def _add(some_tokens: list):
            # Step: Add relationships between the primary token and each inferred token
            for token in some_tokens:
                for i, row in self.df.iterrows():

                    # Rule: Row must have current token as Owner
                    if row["Owner"] != token:
                        continue

                    n.add((token,
                           row["Token"],
                           row["Relationship"]))

        _add(self.df[self.df.Type == 'PrimaryToken']["Token"].unique())
        _add(self.df[self.df.Type == 'InferredToken']["Token"].unique())

        return sorted(n)

    def process(self,
                file_extension: str = "png",
                file_name: str = "skills") -> Digraph:
        # Step: Define the Graph
        f = Digraph(comment='Relationships',
                    format=file_extension,
                    name=file_name)

        # Step: Add Nodes
        f.attr('node',
               shape='doublecircle',
               color='black')

        [f.node(node) for node in self._nodes()]

        # Step: Add Relationships
        [f.edge(rel[0],
                rel[1],
                label=rel[2]) for rel in self._rels()]

        return f
