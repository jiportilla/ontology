#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pprint

from py2neo import Node, Relationship
from py2neo import Transaction

from base import BaseObject
from base import MandatoryParamError


class NeoGraphAugmentation(BaseObject):
    """ add additional entities to the Neo Graph """

    def __init__(self,
                 some_neo_graph,
                 is_debug=False):
        """
        Created:
            25-Feb-2019
            craig.trim@ibm.com
        Updated:
            5-Mar-2019
            craig.trim@ibm.com
            *   refactored
        """
        BaseObject.__init__(self, __name__)
        if not some_neo_graph:
            raise MandatoryParamError("Neo Graph")

        self.neog = some_neo_graph
        self._is_debug = is_debug
        self.jrrids_not_found = set()

    #
    # def _find_one(self,
    #               some_text: str) -> Node:
    #     matcher = NodeMatcher(self.neog)
    #
    #     query = "_.name =~ '{}'".format(some_text)
    #     results = list(matcher.match().where(query).order_by("_.name").limit(3))
    #
    #     if results:
    #         return results[0]

    def _link_to_tags(self,
                      tx: Transaction,
                      an_openseat_node: Node,
                      tagged_entities: list) -> None:
        """
        create relationships that link all tagged entities to the open-seat node
        :param tx:
            the current neo transaction
        :param an_openseat_node:
            the current neo record
        :param tagged_entities:
            all the tagged entities from the mongoDB parsed collection
        """
        from . import NeoUtils

        def _cleanse(some_owl_entity):
            return some_owl_entity.split("#")[-1].replace("_", " ").strip()

        entities = [_cleanse(x) for x in tagged_entities if x]
        if self._is_debug:
            self.logger.debug("\n".join([
                "Located OWL Entities - 1",
                pprint.pformat(entities, indent=4)
            ]))

        entities = [NeoUtils.find_one(self.neog, x) for x in entities if x]
        if self._is_debug:
            self.logger.debug("\n".join([
                "Located OWL Entities - 2",
                pprint.pformat(entities, indent=4)
            ]))

        entities = [x for x in entities if x]
        if self._is_debug:
            self.logger.debug("\n".join([
                "Located OWL Entities - 3",
                pprint.pformat(entities, indent=4)
            ]))

        for entity in entities:
            tx.create(Relationship(an_openseat_node, "mentions", entity))

    def _link_to_jrid(self,
                      tx: Transaction,
                      an_openseat_node: Node,
                      an_openseat_record: dict,
                      is_debug=True):
        """ link an open seat node to a JR ID node """
        from . import NeoUtils

        jr_id = NeoUtils.text_as_jrid(
            an_openseat_record["fields"][11]["value"])
        neo_jr_id = NeoUtils.find_one(self.neog, jr_id)

        if not neo_jr_id:
            self.jrrids_not_found.add(jr_id)

            if is_debug:
                self.logger.warning("\n".join([
                    "JobRole ID Not Found",
                    "\tjr-id: {}".format(jr_id),
                    "\topen-seat-id: {}".format(an_openseat_record["fields"][0]["value"]),
                    "\tjob-role-id: {}".format(jr_id)
                ]))

        else:
            tx.create(Relationship(an_openseat_node, "references", neo_jr_id))

    def process(self,
                openseat_record: dict,
                tagged_entities: list) -> None:
        from . import NeoUtils

        def _open_tx(a_record: Node) -> Transaction:
            a_tx = self.neog.begin()
            a_tx.create(a_record)
            return a_tx

        def _close_tx(a_tx: Transaction):
            """ close the transaction """
            a_tx.commit()

        open_seat_id = openseat_record["fields"][0]["value"]
        openseat_node = NeoUtils.define_node(open_seat_id,
                                             "OpenSeat")

        tx = _open_tx(openseat_node)

        self._link_to_jrid(tx, openseat_node, openseat_record)
        self._link_to_tags(tx, openseat_node, tagged_entities)

        _close_tx(tx)

        if len(self.jrrids_not_found):
            self.logger.warning("\n".join([
                "The Following JR-IDs were not found",
                "\ttotal: {}".format(len(self.jrrids_not_found)),
                "\t{}".format(self.jrrids_not_found)
            ]))
