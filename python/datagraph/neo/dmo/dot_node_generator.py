#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from base import LabelFormatter
from datadict import FindDimensions
from . import DotGraphContext


class DotNodeGenerator(BaseObject):
    """ """

    def __init__(self,
                 context: DotGraphContext,
                 source_records: list,
                 mongo_source: dict,
                 xdm_schema: str = 'supply'):
        """
        Created:
            7-Apr-2019
            craig.trim@ibm.com
            *   based on 'neo-node-generator'
        :param         self.context = context
:
            an existing graph context
        """
        BaseObject.__init__(self, __name__)

        self.context = context
        self.mongo_source = mongo_source
        self.source_records = source_records
        self._dim_finder = FindDimensions(xdm_schema)

    def _schema_element(self,
                        some_text: str) -> str:
        result = self._dim_finder.find(some_text)[0]
        return LabelFormatter.camel_case(result)

    def process(self,
                is_debug=False) -> DotGraphContext:
        """ generate all the nodes"""

        node_entity = "-".join([self.mongo_source["description"]["database"],
                                self.mongo_source["description"]["collection"]])

        for source_record in self.source_records:
            for field in source_record:

                if not field["create_node"]:
                    continue

                def _field_value() -> str:
                    """
                    :return:
                        the properly formatted value for a field
                    """
                    if type(field["value"]) == list:
                        return " ".join(field["value"])
                    return field["value"]

                field_value = _field_value()

                field_node = self.context.find_or_create_node(node_name=field_value,
                                                              node_parent=field["name"],
                                                              provenance={},
                                                              manifest=field["properties"],
                                                              is_debug=is_debug)

                for tag in field["tags"]:
                    _parent = self._schema_element(tag["name"])

                    tag_node = self.context.find_or_create_node(node_name=tag["name"],
                                                                node_parent=_parent,
                                                                manifest=field["properties"],
                                                                provenance={"activity": tag["type"],
                                                                            "entity": tag["provenance"]},
                                                                is_debug=is_debug)

                    self.context.find_or_create_relationships(subject_name=field_node["id"],
                                                              predicate_name="implies-type-1",
                                                              object_name=tag_node["id"],
                                                              provenance={"activity": "Known Relationship"},
                                                              is_debug=is_debug)
                    for tag_link in tag["links"]:
                        tag_link_parent = self._schema_element(tag_link)

                        tag_link_node = self.context.find_or_create_node(node_name=tag_link,
                                                                         node_parent=tag_link_parent,
                                                                         manifest=field["properties"],
                                                                         provenance={"activity": tag["type"],
                                                                                     "entity": tag[
                                                                                         "provenance"]},
                                                                         is_debug=is_debug)

                        self.context.find_or_create_relationships(subject_name=tag_node["id"],
                                                                  predicate_name="implies-type-2",
                                                                  object_name=tag_link_node["id"],
                                                                  provenance={"entity": "Vector Space",
                                                                              "activity": "Inferred Relationships"},
                                                                  is_debug=is_debug)

        return self.context
