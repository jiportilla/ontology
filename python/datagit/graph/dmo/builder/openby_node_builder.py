# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from datagit.graph.dmo.util import GraphNodeIdGenerator
from datagit.graph.dmo.util import GraphNodeDefGenerator
from datagit.graph.dmo.util import GraphTextSplitter
from datamongo import CendantRecordParser


class OpenByNodeBuilder(BaseObject):
    """ Build a GitHub OpenBy Node for Graphviz """

    def __init__(self,
                 d_record: dict,
                 stylesheet_path: str,
                 is_debug: bool = True):
        """
        Created:
            25-Dec-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1646
        Updated:
            31-Dec-2019
            craig.trim@ibm.com
            *   add cendant-record-parser
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1681#issuecomment-16873873
            *   use node line generator to support stylesheet pathing
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1681#issuecomment-16877187
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
        self._line_generator = GraphNodeDefGenerator(is_debug=self._is_debug,
                                                     stylesheet_path=stylesheet_path)

        self.state = {}
        self._build_state()

    def _build_state(self) -> None:
        def node_id() -> str:
            issue_id = self._d_record['key_field']
            return GraphNodeIdGenerator(a_type='person',
                                        a_label=issue_id).process()

        def label() -> str:
            name = self._parser.field_value_by_name(self._d_record, 'OpenBy')
            name = name.replace('-', ' ')
            return GraphTextSplitter.split_text(name, threshold=10)

        node_id = node_id()
        label = label()
        def lines() -> list:
            return self._line_generator.process(node_id=node_id,
                                            node_type='person',
                                            node_label=label,
                                            comment=str(self.__class__.__name__))

        self.state = {
            "node_id": node_id,
            "lines": lines}
