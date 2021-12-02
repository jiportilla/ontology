#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from py2neo import Transaction

from base import BaseObject
from base import MandatoryParamError
from datagraph.neo.dmo import NeoGraphContext
from datagraph.neo.dmo import NeoUtils


class GenerateSimilarityMetric(BaseObject):
    """ """

    def __init__(self,
                 some_tx: Transaction,
                 some_graph_context: NeoGraphContext,
                 use_categorical_relationships=True):
        """
        Created:
            19-Mar-2019
            craig.trim@ibm.com
            *   refactored out of 'load-neo-from-manifest-2'
        :param some_tx:
            an active neo transaction
        :param some_graph_context:
            an active neo transaction
        :param use_categorical_relationships:
            if True, use relationships like
                'lowest-similarity',
                'is-similar',
                ...
                'highest-similarity'
            if False, just use
                'is-similar'
        """
        BaseObject.__init__(self, __name__)
        if not some_tx:
            raise MandatoryParamError("Neo Transaction")
        if not some_graph_context:
            raise MandatoryParamError("Graph Context")

        self.tx = some_tx
        self.graph_context = some_graph_context
        self.categorical_relationships = use_categorical_relationships

    def _relationship_name(self,
                           total: int) -> str:
        def _to_categorical():
            switcher = {
                1: "lowest-similarity",
                2: "low-similarity",
                3: "is-similar",
                4: "high-similarity",
                5: "highest-similarity"
            }
            if total > 5:
                return switcher[5]
            return switcher[total]

        if self.categorical_relationships:
            return _to_categorical()
        return "is-similar"

    def _similarity_metric(self,
                           d_tags: dict):

        # create similarity metric
        s_complete = set()

        for tag1 in d_tags:
            for tag2 in d_tags:
                if tag1 == tag2:
                    continue

                total = len(set(tag1.split(" ")).intersection(set(tag2.split(" "))))
                if total > 1:

                    # prevent dupes (e.g. bidirectional flows)
                    key = "".join(sorted({tag1, tag2}))
                    if key in s_complete:
                        continue

                    s_complete.add(key)

                    _s = self.graph_context.find_or_create_node(self.tx,
                                                                tag1,
                                                                d_tags[tag1])
                    _p = self._relationship_name(total)

                    _o = self.graph_context.find_or_create_node(self.tx,
                                                                tag2,
                                                                d_tags[tag2])

                    NeoUtils.define_relationship(_s, _p, _o,
                                                 activity="Probabilistic Relationship",
                                                 entity="Vector Space")

    def _generate_graph_dictionary(self) -> dict:
        """
        the graph_context contains a dictionary of actual neo4j nodes
        this routine takes that dictionary and creates a simple key:value dictionary of graph nodes to graph types
        for example:
            {   'Server Support Specialist': 'Domain Term',
                'AIX': 'Operating System' }
            etc
        :return:
        """
        d_graph = {}
        keys = sorted(self.graph_context.node_lookup.keys())

        for node_key in keys:
            node_name = node_key.split("-")[0].strip()
            node_type = node_key.split("-")[-1].strip()
            d_graph[node_name] = node_type

        return d_graph

    def process(self):
        self._similarity_metric(
            self._generate_graph_dictionary())
