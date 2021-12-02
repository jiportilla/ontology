#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from . import DotGraphContext


class DotRelationshipGenerator(BaseObject):
    """ """

    def __init__(self,
                 source_records: list,
                 context: DotGraphContext):
        """
        Created:
            7-Apr-2019
            craig.trim@ibm.com
            *   based on 'neo-relationship-generator'
        :param context:
            an existing graph context
        """
        BaseObject.__init__(self, __name__)

        self.context = context
        self.source_records = source_records

    def process(self) -> DotGraphContext:
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

                _subject = self.context.find_or_create_node(node_name=_field_value(),
                                                            node_parent=field["name"],
                                                            manifest={},
                                                            provenance={},
                                                            is_debug=True)

                for rel in field["relationships"]:

                    def _rel_value() -> str:
                        if type(rel["value"]) == list:
                            return " ".join(rel["value"])
                        return rel["value"]

                    _object = self.context.find_or_create_node(node_name=_rel_value(),
                                                               node_parent=rel["name"],
                                                               manifest={},
                                                               provenance={},
                                                               is_debug=True)

                    self.context.find_or_create_relationships(subject_name=_subject["id"],
                                                              predicate_name=rel["type"],
                                                              object_name=_object["id"],
                                                              provenance={"activity": "Known Relationship"},
                                                              is_debug=True)

        return self.context
