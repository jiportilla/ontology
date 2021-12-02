#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from py2neo import Relationship
from py2neo import Transaction

from base import BaseObject
from base import LabelFormatter
from datadict import FindDimensions
from . import NeoGraphContext


class NeoNodeGenerator(BaseObject):
    """ """

    def __init__(self,
                 some_tx: Transaction,
                 context: NeoGraphContext,
                 source_records: list,
                 mongo_source: dict,
                 xdm_schema: str = 'supply'):
        """
        Created:
            5-Apr-2019
            craig.trim@ibm.com
            *   refactored out of 'initialize-neo-graph'
        :param some_tx:
            an active Neo transaction
        :param context:
            an existing graph context
        """
        BaseObject.__init__(self, __name__)

        self.tx = some_tx
        self.context = context
        self.mongo_source = mongo_source
        self.source_records = source_records
        self._dim_finder = FindDimensions(xdm_schema)

    def _schema_element(self,
                        some_text: str) -> str:
        result = self._dim_finder.find(some_text)[0]
        return LabelFormatter.camel_case(result)

    def process(self) -> NeoGraphContext:
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
                field_node = self.context.find_or_create_node(self.tx,
                                                              field_value,
                                                              field["name"],
                                                              some_entity=node_entity,
                                                              some_manifest_properties=field["properties"])

                for tag in field["tags"]:
                    _parent = self._schema_element(tag["name"])
                    tag_node = self.context.find_or_create_node(self.tx,
                                                                tag["name"],
                                                                _parent,
                                                                some_activity=tag["type"],
                                                                some_entity=tag["provenance"],
                                                                some_manifest_properties=field["properties"])
                    self.tx.create(Relationship(field_node,
                                                "implies-type-1",
                                                tag_node,
                                                entity="",
                                                activity="Known Relationship"))

                # add subsumes implications
                for tag in field["tags"]:
                    _s_parent = self._schema_element(tag["name"])
                    _s = self.context.find_or_create_node(self.tx,
                                                          tag["name"],
                                                          _s_parent,
                                                          some_activity=tag["type"],
                                                          some_entity=tag["provenance"],
                                                          some_manifest_properties=field["properties"])

                    for tag_link in tag["links"]:
                        _o_parent = self._schema_element(tag_link)
                        _o = self.context.find_or_create_node(self.tx,
                                                              tag_link,
                                                              _o_parent,
                                                              some_activity=tag["type"],
                                                              some_entity=tag["provenance"],
                                                              some_manifest_properties=field["properties"])

                        self.tx.create(Relationship(_s,
                                                    "implies-type-2",
                                                    _o,
                                                    entity="Vector Space",
                                                    activity="Inferred Relationship"))

        return self.context
