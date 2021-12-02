# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from datagit.graph.dmo.util import GraphNodeIdGenerator
from datamongo import CendantRecordParser


class EpicAssignmentNodeBuilder(BaseObject):
    """ Build GitHub Epic Assignment Node(s) for Graphviz """

    def __init__(self,
                 d_record: dict,
                 is_debug: bool = True):
        """
        Created:
            26-Dec-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1667#issuecomment-16821672
        Updated:
            31-Dec-2019
            craig.trim@ibm.com
            *   refactor node-id
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1681#issuecomment-16877316
        Updated:
            2-Jan-2020
            craig.trim@ibm.com
            *   refactor code into state/build-state standard
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1681#issuecomment-16877328
        :param is_debug:
            if True     increase log output at DEBUG level
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._d_record = d_record
        self._parser = CendantRecordParser(is_debug=self._is_debug)

        self.state = {"node_ids": []}
        self._build_state()

    def _build_state(self):
        assignments = self._parser.field_values_by_name(a_record=self._d_record,
                                                        a_field_name='EpicAssignment')

        def _node_id(an_assignment: str) -> str:
            return GraphNodeIdGenerator(a_type='issue',
                                        a_label=an_assignment).process()

        node_ids = [_node_id(assignment) for assignment in assignments]
        [self.state["node_ids"].append(node_id) for node_id in node_ids]
