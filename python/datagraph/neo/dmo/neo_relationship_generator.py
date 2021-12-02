#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from py2neo import Relationship
from py2neo import Transaction

from base import BaseObject
from . import NeoGraphContext


class NeoRelationshipGenerator(BaseObject):
    """ """

    def __init__(self,
                 source_records: list,
                 some_tx: Transaction,
                 context: NeoGraphContext, ):
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
        self.source_records = source_records

    def process(self) -> NeoGraphContext:
        """ generate all the relationships
        """

        for source_record in self.source_records:

            for field in source_record:

                if not field["create_node"]:
                    continue

                def _field_value() -> str:
                    if type(field["value"]) == list:
                        return " ".join(field["value"])
                    return field["value"]

                _subject = self.context.find_or_create_node(self.tx,
                                                            _field_value(),
                                                            field["name"], )

                for rel in field["relationships"]:

                    def _rel_value() -> str:
                        if type(rel["value"]) == list:
                            return " ".join(rel["value"])
                        return rel["value"]

                    _object = self.context.find_or_create_node(self.tx,
                                                               _rel_value(),
                                                               rel["name"])
                    self.tx.create(Relationship(_subject,
                                                rel["type"],
                                                _object,
                                                activity="Known Relationship"))

        return self.context
