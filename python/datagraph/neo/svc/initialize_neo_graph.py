#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import rdflib
from py2neo import Node
from py2neo import Relationship
from py2neo import Transaction

from base import BaseObject
from base import MandatoryParamError
from datadict import FindDimensions
from datadict import FindEntity
from datadict import FindRelationships


class InitializeNeoGraph(BaseObject):
    """ Initialize the neo4j graph with the ontology model

        **** WARNING ****
        Running this service will wipe all existing data from the neo4j graph """

    def __init__(self,
                 some_owl_graph: rdflib.Graph,
                 some_connection_type: str = 'local',
                 xdm_schema: str = 'supply'):
        """
        Created:
            21-Feb-2019
            craig.trim@ibm.com
        Updated:
            23-Feb-2019
            craig.trim@ibm.com
            *   added yml creds
        Updated:
            25-Feb-2019
            craig.trim@ibm.com
            *   invoke from API
        Updated:
            26-Mar-2019
            craig.trim@ibm.com
            *   updated based on large-scale MDA changes
        Updated:
            29-Oct-2019
            craig.trim@ibm.com
            *   remove 'entity-schema-finder' in favor of new approach
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/916#issuecomment-15620801
        :param some_owl_graph:
        :param some_connection_type:
        :param xdm_schema:
            the name of the schema to perform the type lookup
            Notes:
            -   typically either 'supply' or 'learning'
            -   the full list is on path 'resources/config/dimensionality'
        """
        BaseObject.__init__(self, __name__)
        if not some_owl_graph:
            raise MandatoryParamError("Owl Graph")

        self._owlg = some_owl_graph
        self._entity_finder = FindEntity()
        self._connection_type = some_connection_type
        self._rel_finder = FindRelationships()
        self._dim_finder = FindDimensions(xdm_schema)

    @staticmethod
    def _add_rels_by_name(tx: Transaction,
                          nodes: dict,
                          some_rels_dict: dict,
                          the_rel_name: str) -> None:
        """
        create relationships between nodes in neo
        :param tx:
            an open neo transaction
        :param nodes:
            the previously created neo4j nodes that will be used to form these relationships
        :param some_rels_dict:
            the dictionary that contains the subject/object values
        :param the_rel_name:
            the name of the relationship in neo
        """
        for k in some_rels_dict:
            if k not in nodes:
                continue

            s = nodes[k]
            for value in some_rels_dict[k]:
                o = nodes[value]
                tx.create(Relationship(s, the_rel_name, o))

    @staticmethod
    def _add_relationships(tx: Transaction,
                           node_lookup: dict,
                           some_rel_dict: dict,
                           some_rel_name: str):
        for k in some_rel_dict:
            s = node_lookup[k]
            for value in some_rel_dict[k]:
                o = node_lookup[value]
                tx.create(Relationship(s, some_rel_name, o))

    def _add_all_relationships(self,
                               tx: Transaction,
                               node_lookup: dict) -> None:
        self._add_relationships(tx,
                                node_lookup,
                                self._rel_finder.all_owns(bidirectional=False),
                                "owns")
        self._add_relationships(tx,
                                node_lookup,
                                self._rel_finder.all_implies(bidirectional=False),
                                "implies")
        self._add_relationships(tx,
                                node_lookup,
                                self._rel_finder.all_versions(bidirectional=False),
                                "hasVersion")
        self._add_relationships(tx,
                                node_lookup,
                                self._rel_finder.all_requires(bidirectional=False),
                                "requires")
        self._add_relationships(tx,
                                node_lookup,
                                self._rel_finder.all_similar(bidirectional=False),
                                "similarTo")
        self._add_relationships(tx,
                                node_lookup,
                                self._rel_finder.all_parts(bidirectional=False),
                                "partOf")
        self._add_relationships(tx,
                                node_lookup,
                                self._rel_finder.all_defines(bidirectional=False),
                                "definedBy")

    def _add_child_of_rels(self,
                           tx: Transaction,
                           node_lookup: dict):

        for parent in self._entity_finder.d_parents_revmap():
            for child in self._entity_finder.children(parent):
                s = node_lookup[child]
                o = node_lookup[parent]
                tx.create(Relationship(s, "childOf", o))

    def _define_all_nodes(self,
                          tx: Transaction) -> dict:
        """ define all the nodes that will be used in the graph """
        node_lookup = {}
        root_lookup = {}

        def _schema_entity(some_input: str) -> str:
            return [x.replace(' ', '') for x in self._dim_finder.find(some_input)][0]

        d_parents = self._entity_finder.d_parents_revmap()

        for k in d_parents:
            root_lookup[k] = _schema_entity(k)

            for value in d_parents[k]:
                root_lookup[value] = _schema_entity(value)

        for k in d_parents:
            node_lookup[k] = Node(root_lookup[k], name=k)

            for value in d_parents[k]:
                node_lookup[value] = Node(root_lookup[value], name=value)

        [tx.create(x) for x in node_lookup.values() if x]

        return node_lookup

    def process(self) -> dict:
        from datagraph.neo.dmo import NeoGraphConnector

        neog = NeoGraphConnector.connect(connection_type=self._connection_type)

        tx = neog.begin()
        neog.delete_all()

        nodes = self._define_all_nodes(tx)
        self.logger.debug(f"Initialized Graph ("
                          f"total-nodes={len(nodes)})")

        self._add_child_of_rels(tx, nodes)
        self._add_all_relationships(tx, nodes)

        tx.commit()
        return nodes


if __name__ == "__main__":
    from datagraph import OwlGraphConnector

    owlg = OwlGraphConnector("cendant").process()
    InitializeNeoGraph(owlg).process()
