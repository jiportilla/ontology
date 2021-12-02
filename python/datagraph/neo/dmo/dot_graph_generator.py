#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import codecs
import os

from base import BaseObject
from datadict import FindDimensions
from . import DotGraphContext


class DotGraphGenerator(BaseObject):
    """ """

    def __init__(self,
                 context: DotGraphContext,
                 xdm_schema: str = 'supply'):
        """
        Created:
            7-Apr-2019
            craig.trim@ibm.com
            *   based on 'neo-node-generator'
        Updated:
            29-Oct-2019
            craig.trim@ibm.com
            *   remove 'entity-schema-finder' in favor of new approach
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/916#issuecomment-15620801
        :param context:
            an existing graph context
        :param xdm_schema:
            the name of the schema to perform the type lookup
            Notes:
            -   typically either 'supply' or 'learning'
            -   the full list is on path 'resources/config/dimensionality'
        """
        BaseObject.__init__(self, __name__)
        self._context = context
        self._dim_finder = FindDimensions(xdm_schema)

    def process(self):
        # pprint.pprint(self.context.node_lookup)
        # pprint.pprint(self.context.predicate_lookup)

        path = os.path.join(os.environ["DESKTOP"], "kb.giz")
        target = codecs.open(path, "w", encoding="utf-8")

        # dot = Digraph(comment='Knowledge Graph', format='png')
        records = ["graph G {"]

        records.append("\n\n\n")
        records.append("# ---- Nodes ----")
        records.append("\n\n\n")

        def _label(some_value: str) -> str:
            return some_value  # .replace(" ", "n")

        def _id(some_id: str) -> str:
            return "HC_{}".format(some_id)

        def _color(some_parent: str) -> str:
            some_parent = some_parent.lower().strip()
            if some_parent == "database":
                return "/ylorrd9/9"
            elif some_parent == "cloud":
                return "/ylorrd9/8"
            elif some_parent == "system administrator":
                return "/ylorrd9/7"
            elif some_parent == "data science":
                return "/ylorrd9/6"
            elif some_parent == "hard skill":
                return "/ylorrd9/5"
            elif some_parent == "service management":
                return "/ylorrd9/4"
            elif some_parent == "project management":
                return "/ylorrd9/3"
            elif some_parent == "soft skill":
                return "/ylorrd9/2"
            return "/ylorrd9/1"

        def _cv_node(node_id: str) -> str:
            return """
                {0} [   label="cv", 
                        fixedsize=true, 
                        fontsize=6, 
                        fontname=Calibri, 
                        shape=record, 
                        width=.1, 
                        height=.1, 
                        style=filled,
                        color=white]
            """.format(node_id).strip()

        def _data_node(node_id: str,
                       node_label: str,
                       node_color: str) -> str:
            return """
                {0} [   label="{1}", 
                        fixedsize=true, 
                        fontsize=6, 
                        fontname=Calibri, 
                        shape=record, 
                        width=.50, 
                        height=.25, 
                        style=filled,
                        color="{2}"]
            """.format(node_id,
                       node_label,
                       node_color).strip()

        def _has_parent(subject_id: str,
                        object_id: str) -> str:
            return """
                {0} -> {1} [    label="", 
                                style=dotted,
                                fontsize=5,
                                fontname=Arial,
                                labelfontsize=5,
                                labelfontname=Arial,
                                labelfloat=true,
                                labeldistance=1.0,
                                color="slategray2"]
            """.format(subject_id,
                       object_id).strip()

        def _relationship(subject_id: str,
                          object_id: str,
                          label: str) -> str:

            def _edge_color():
                if label == "implies-type-1":
                    return "red"
                elif label == "implies-type-2":
                    return "blue"
                return "black"

            return """
                {0} -> {1} [    label="", 
                                style=bold,
                                fontsize=5,
                                fontname=Arial,
                                labelfontsize=6,
                                labelfontname=Arial,
                                labelfloat=true,
                                labeldistance=1.0,
                                color={2}]
            """.format(subject_id,
                       object_id,
                       _edge_color()).strip()

        ctr = 0
        for k in self._context.node_lookup():
            node = self._context.node_lookup()[k]

            _node_id = _id(node["id"])
            _node_label = _label(node["node"]["name"])

            _parent_id = _label(node["node"]["parent"])
            _parent_label = _parent_id

            _node_color = _color(_parent_label)

            if len(_node_label) > 20:  # pass-through node
                records.append(_cv_node(_node_id))
            else:
                records.append(_data_node(_node_id,
                                          _node_label,
                                          _node_color))

            records.append(_data_node(_parent_id,
                                      _parent_label,
                                      _node_color))

            records.append(_has_parent(_node_id,
                                       _parent_id))

            ctr += 1

        records.append("\n\n\n")
        records.append("# ---- Relationships ----")
        records.append("\n\n\n")

        ctr = 0
        for k in self._context.predicate_lookup():
            predicate = self._context.predicate_lookup()[k]

            records.append(_relationship(_id(predicate["triple"]["subject"]),
                                         _id(predicate["triple"]["object"]),
                                         predicate["triple"]["predicate"]))

            ctr += 1

        records.append("}")

        target.write("\n".join(records))
        target.close()
