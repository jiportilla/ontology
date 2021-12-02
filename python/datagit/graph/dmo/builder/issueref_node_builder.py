# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from datagit.graph.dmo.util import GraphNodeIdGenerator
from datamongo import CendantRecordParser


class IssueReferenceNodeId(BaseObject):
    """ Build a GitHub Issue reference Node for Graphviz """

    def __init__(self,
                 d_record: dict,
                 is_debug: bool = True):
        """
        Created:
            26-Dec-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/-cdo/unstructured-analytics/issues/1647#issuecomment-16821234
        Updated:
            31-Dec-2019
            craig.trim@ibm.com
            *   add cendant-record-parser
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1681#issuecomment-16873873
            *   use node line generator to support stylesheet pathing
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1681#issuecomment-16877187
            *   refactor node-id
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1681#issuecomment-16877316
        :param is_debug:
            if True     increase log output at DEBUG level
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._d_record = d_record

        self.node_ids = []
        self._process(d_record)

    def _process(self,
                 d_record: dict):
        parser = CendantRecordParser(is_debug=self._is_debug)
        body = parser.field_value_by_name(a_record=d_record,
                                          a_field_name='Body')

        tokens = [token.strip() for token in body.split(' ')]
        tokens = [token for token in tokens if token and len(token)]
        tokens = [token for token in tokens if token.startswith('#')]
        tokens = [str(token[1:]) for token in tokens if token[1:].isdigit()]

        def node_id(a_reference: str) -> str:
            return GraphNodeIdGenerator(a_type='issue',
                                        a_label=a_reference).process()

        node_ids = [node_id(ref) for ref in tokens]
        [self.node_ids.append(node_id) for node_id in node_ids]
