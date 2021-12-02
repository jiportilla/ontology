#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import time
from datetime import date

from py2neo import Graph
from py2neo import Node
from py2neo import NodeMatcher
from py2neo import Relationship
from py2neo import Transaction


class DotUtils(object):
    """
    Created:
        7-Apr-2019
        craig.trim@ibm.com
        *   based on 'neo-utils'
    """

    @staticmethod
    def loaded_on():
        today = date.today().strftime("%B %d, %Y")
        return "Loaded on {}".format(today)

    @staticmethod
    def text_as_jrid(some_text: str) -> str:
        """ define as text string as valid JobRoleId """

        some_text = str(some_text)
        if "." in some_text:
            some_text = some_text.split(".")[0].strip()

        if "JR_ID_" not in some_text:
            return "JR_ID_{}".format(str(some_text))

        return some_text

    @staticmethod
    def text_as_srid(some_text: str) -> str:
        """ define as text string as valid SkillSetId """

        some_text = str(some_text)
        if "." in some_text:
            some_text = some_text.split(".")[0].strip()

        if "SR_ID_" not in some_text:
            return "SR_ID_{}".format(str(some_text))

        return some_text

    @staticmethod
    def define_node_jrid(some_id: str):
        """ Create a JobRoleId node """
        return NeoUtils.define_node(NeoUtils.text_as_jrid(some_id),
                                    "JobRoleId")

    @staticmethod
    def define_node_srid(some_id: str):
        """ Create a SkillSetID node """
        return NeoUtils.define_node(NeoUtils.text_as_srid(some_id),
                                    "SkillSetId")

    @staticmethod
    @DeprecationWarning
    def define_node(node_name: str,
                    node_type: str,
                    activity=None,
                    agent=None,
                    entity=None):
        """ define a neo node """

        return Node(node_type,
                    name=node_name,
                    activity=activity,
                    entity=entity,
                    agent=agent,
                    date=NeoUtils.loaded_on())

    # @staticmethod
    # def define_node_2(node_name: str,
    #                   node_type: str,
    #                   properties:dict):
    #     """ define a neo node """
    #
    #     return Node(node_type,
    #                 name=node_name,
    #                 properties=properties,
    #                 date=NeoUtils.loaded_on())

    @staticmethod
    def define_relationship(s: Node,
                            p: str,
                            o: Node,
                            activity=None,
                            agent=None,
                            entity=None):
        """ define a relationship """
        return Relationship(s, p, o,
                            activity=activity,
                            entity=entity,
                            agent=agent,
                            when=NeoUtils.loaded_on())

    @staticmethod
    def find(some_graph: Graph,
             some_text: str,
             limit=3) -> list:
        """ search by text to find a list of matches with cardinality 0..* """

        matcher = NodeMatcher(some_graph)
        query = "_.name =~ '{}'".format(some_text)
        return list(matcher.match().where(query).order_by("_.name").limit(limit))

    @staticmethod
    def find_one(some_graph: Graph,
                 some_text: str) -> Node:
        """ return the first match by text (if any) """

        results = NeoUtils.find(some_graph, some_text)
        if results and len(results):
            return results[0]

    @staticmethod
    def find_jrid(some_graph: Graph,
                  some_id: str) -> Node:
        """ return the first Job Role ID Node that matches by id (if any) """

        results = NeoUtils.find(some_graph,
                                NeoUtils.text_as_jrid(some_id))

        if results and len(results):
            return results[0]

    @staticmethod
    def query(tx: Transaction,
              query: str) -> dict:
        """ query an active neo transaction and transform the results to a dictionary """
        nodes = []

        def _retkey():
            # e.g. 'MATCH (n:EmployeeCnum) RETURN n' need to know 'n'
            return query.split("RETURN ")[-1].strip()

        for node in [x[_retkey()] for x in tx.run(query)]:

            d_node = {}
            for x in iter(node):
                d_node[x] = node[x]

            nodes.append(d_node)

        return {
            "tts": time.time(),
            "query": query,
            "results": nodes
        }
