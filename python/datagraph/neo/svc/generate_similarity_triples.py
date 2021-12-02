#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from base import MandatoryParamError
from datagraph.neo.dmo import NeoUtils


class GenerateSimilarityTriples(BaseObject):
    """ """

    def __init__(self,
                 some_nodes: dict,
                 some_rels: dict,
                 use_categorical_relationships=True):
        """
        Created:
            19-Mar-2019
            craig.trim@ibm.com
            *   refactored out of 'load-neo-from-manifest-2'
        :param some_nodes:
            node dictionary with hash keys
        :param some_rels:
            rel dictionary with hash keys
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
        if not some_nodes:
            raise MandatoryParamError("Nodes")
        if not some_rels:
            raise MandatoryParamError("Relationships")

        self.d_rels = some_rels
        self.d_nodes = some_nodes
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

    def _find_or_create_node(self,
                             node_name: str,
                             node_type: str,
                             some_activity=None,
                             some_entity=None) -> str:
        key = str(hash("{}-{}".format(node_name, node_type)))
        if key not in self.d_nodes:
            self.d_nodes[key] = {
                "name": node_name,
                "type": node_type,
                "meta": {
                    "activity": some_activity,
                    "entity": some_entity,
                    "when": NeoUtils.loaded_on()
                }
            }

        return key

    def _find_or_create_rel(self,
                            s: str,
                            p: str,
                            o: str,
                            some_activity=None,
                            some_entity=None):
        key = str(hash("{}-{}-{}".format(s, p, o)))
        if key not in self.d_rels:
            self.d_rels[key] = {
                "subject": s,
                "object": o,
                "meta": {
                    "activity": some_activity,
                    "entity": some_entity,
                    "when": NeoUtils.loaded_on()
                }
            }

        return key





def process(self):
    self._similarity_metric(
        self._generate_graph_dictionary())
