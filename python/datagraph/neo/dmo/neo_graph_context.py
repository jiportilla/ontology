#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from py2neo import Node
from py2neo import Transaction

from base import BaseObject


class NeoGraphContext(BaseObject):
    """ """

    def __init__(self):
        """
        Created:
            18-Mar-2019
            craig.trim@ibm.com
            *   refactored out of 'load-neo-from-manifest-2'
        """
        BaseObject.__init__(self, __name__)
        self.node_lookup = {}

    @staticmethod
    def _key_by_name_value(a_name: str,
                           a_value: str):
        return "{}-{}".format(str(a_name),
                              str(a_value))

    def total_nodes(self) -> int:
        return len(self.node_lookup)

    def find_or_create_node(self,
                            tx: Transaction,
                            some_name: str,
                            some_parent,
                            some_agent=None,
                            some_entity=None,
                            some_activity=None,
                            some_manifest_properties=None) -> Node:
        """ find a node in cache or create and add to cache """
        from . import NeoUtils

        _key = self._key_by_name_value(some_name,
                                       some_parent)

        # return node from cache
        if _key in self.node_lookup:
            return self.node_lookup[_key]

        def _labels() -> list:
            return [str(some_parent)]

        def _properties() -> dict:
            """
            define the node properties
            1.  each node has at least a 'name' and a 'date' (date loaded on)
            2.  each node may have provenance properties:
                a.  agent
                b.  entity
                c.  activity
            3.  each node may have manifest proeprties
            :return:
                a dictionary of properties
            """
            properties = {"name": str(some_name),
                          "date": NeoUtils.loaded_on()}
            if some_manifest_properties:
                for p in some_manifest_properties:
                    properties[p["name"]] = p["value"]
            if some_activity:
                properties["agent"] = some_agent
            if some_activity:
                properties["entity"] = some_entity
            if some_activity:
                properties["activity"] = some_activity
            return properties

        _node = Node(*_labels(),
                     **_properties())

        tx.create(_node)
        self.node_lookup[_key] = _node

        return _node

    def explain(self):
        total_nodes = len(self.node_lookup)

        self.logger.debug("\n".join([
            "Graph Context Insight",
            "\ttotal-nodes: {}".format(total_nodes)
        ]))
