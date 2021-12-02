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


class IssueNodeBuilder(BaseObject):
    """ Build a GitHub Issue Node for Graphviz """

    def __init__(self,
                 d_record: dict,
                 stylesheet_path: str,
                 df_social_entity_analysis: Optional[DataFrame],
                 is_debug: bool = True):
        """
        Created:
            19-Dec-2019
            craig.trim@ibm.com
            *   refactored out of 'graph-github-issue'
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1631
        Updated:
            25-Dec-2019
            craig.trim@ibm.com
            *   update node line generation
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1654
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
        self._parser = CendantRecordParser(is_debug=self._is_debug)
        self._line_generator = GraphNodeDefGenerator(is_debug=self._is_debug,
                                                     stylesheet_path=stylesheet_path)

        self.state = {}
        self._build_state()

        if self._is_debug:
            self.logger.debug('\n'.join([
                "IssueNodeBuilder Summary",
                f"\tKeys: {sorted(self.state.keys())}"]))

    def _nodesize(self,
                  person_name: str) -> dict:
        generator = SocialNodeSizeGenerator(is_debug=self._is_debug,
                                            df=self._df_social_entity_analysis)
        return generator.process(person_name)

    def _build_issue(self) -> dict:
        def node_id() -> str:
            return GraphNodeIdGenerator(a_type='issue',
                                        a_label=self._d_record['key_field']).process()

        def label() -> str:
            issue_title = self._parser.field_value_by_name(self._d_record, 'Title')
            issue_title = GraphTextSplitter.split_text(issue_title, threshold=10)

            template = "{Issue|#ID}|#LABEL"
            template = template.replace("#ID", self._d_record['key_field'])
            template = template.replace("#LABEL", issue_title)

            return template

        label = label()
        node_id = node_id()

        def lines() -> list:
            return self._line_generator.process(node_id=node_id,
                                                node_type='issue',
                                                node_label=label,
                                                comment=str(self.__class__.__name__))

        return {
            "lines": lines(),
            "node_id": node_id}

    def _build_person_node(self,
                           name: str) -> dict:
        def node_id() -> str:
            return GraphNodeIdGenerator(a_type='person',
                                        a_label=name).process()

        def label() -> str:
            the_label = name.replace('-', ' ')
            return GraphTextSplitter.split_text(the_label, threshold=5)

        node_id = node_id()
        label = label()
        nodesize = self._nodesize(name)

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
            "person_name": name}

    def _build_person_nodes(self):
        for relationship in ["OpenBy", "CloseBy", "Assignees"]:

            def build() -> Optional[dict]:
                name = self._parser.field_value_by_name(self._d_record, relationship)
                if name:
                    return self._build_person_node(name)

            self.state[relationship] = build()

    def _build_state(self):
        self.state["issue"] = self._build_issue()
        self._build_person_nodes()
