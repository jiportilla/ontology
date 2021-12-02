# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from typing import Optional

from pandas import DataFrame

from base import BaseObject
from datagit.graph.dmo.util import GraphNodeDefGenerator
from datagit.graph.dmo.util import GraphNodeIdGenerator
from datagit.graph.dmo.util import GraphTextSplitter
from datagit.graph.dmo.util import SocialNodeSizeGenerator
from datamongo import CendantRecordParser


class AssigneeNodeBuilder(BaseObject):
    """ Build a GitHub Assignee Node for Graphviz """

    def __init__(self,
                 d_record: dict,
                 stylesheet_path: str,
                 df_social_entity_analysis: Optional[DataFrame],
                 is_debug: bool = True):
        """
        Created:
            25-Dec-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1646
        Updated:
            30-Dec-2019
            craig.trim@ibm.com
            *   refactored state and node building in pursuit of
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1678
        Updated:
            31-Dec-2019
            craig.trim@ibm.com
            *   add cendant-record-parser
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1681#issuecomment-16873873
            *   use node line generator to support stylesheet pathing
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1681#issuecomment-16877187
            *   refactor node-id
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1681#issuecomment-16877316
        Updated:
            2-Jan-2020
            craig.trim@ibm.com
            *   use social entity analysis data frame to influence nodesize
                https://github.ibm.com/-cdo/unstructured-analytics/issues/1680#issuecomment-16901723
        :param is_debug:
            if True     increase log output at DEBUG level
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._d_record = d_record
        self._df_social_entity_analysis = df_social_entity_analysis
        self._line_generator = GraphNodeDefGenerator(is_debug=self._is_debug,
                                                     stylesheet_path=stylesheet_path)

        self.state = {}
        self._build_state()

    def _nodesize(self,
                  person_name: str) -> dict:
        generator = SocialNodeSizeGenerator(is_debug=self._is_debug,
                                            df=self._df_social_entity_analysis)
        return generator.process(person_name)

    def _build_assignee_node(self):
        parser = CendantRecordParser(is_debug=self._is_debug)
        person_name = parser.field_value_by_names(self._d_record, ['Assignee', 'Assignees'])

        def node_id() -> str:
            return GraphNodeIdGenerator(a_type='person',
                                        a_label=person_name).process()

        def label() -> str:
            the_label = person_name.replace('-', ' ')
            return GraphTextSplitter.split_text(the_label, threshold=5)

        node_id = node_id()
        label = label()

        nodesize = self._nodesize(person_name)

        def lines() -> list:
            return self._line_generator.process(node_id=node_id,
                                                node_type='person',
                                                node_label=label,
                                                height=nodesize['height'],
                                                width=nodesize['width'],
                                                comment=str(self.__class__.__name__))

        return {
            "lines": lines(),
            "node_id": node_id,
            "person_name": person_name}

    def _build_state(self):
        self.state["assignee"] = self._build_assignee_node()
