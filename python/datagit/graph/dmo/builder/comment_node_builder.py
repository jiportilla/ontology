# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from typing import Optional

from pandas import DataFrame

from base import BaseObject
from datagit.analyze.dmo import CommentMentionExtractor
from datagit.graph.dmo.util import GraphNodeDefGenerator
from datagit.graph.dmo.util import GraphNodeIdGenerator
from datagit.graph.dmo.util import GraphTextSplitter
from datagit.graph.dmo.util import SocialNodeSizeGenerator
from datamongo import CendantRecordParser


class CommentNodeBuilder(BaseObject):
    """ Build a Python Comments Node for Graphviz """

    def __init__(self,
                 d_record: dict,
                 stylesheet_path: str,
                 df_social_entity_analysis: Optional[DataFrame],
                 is_debug: bool = True):
        """
        Created:
            26-Dec-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/-cdo/unstructured-analytics/issues/1646#issuecomment-16820924
        Updated:
            30-Dec-2019
            craig.trim@ibm.com
            *   refactored state and node building in pursuit of
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1678
        Updated:
            31-Dec-2019
            *   use comment-mention-extractor
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1681#issuecomment-16873584
            *   add cendant-record-parser
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1681#issuecomment-16873873
            *   refactor node-id
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1681#issuecomment-16877316
        Updated:
            2-Jan-2020
            craig.trim@ibm.com
            *   use social entity analysis data frame to influence nodesize
                https://github.ibm.com/-cdo/unstructured-analytics/issues/1680#issuecomment-16901723
        :param d_record:
        :param is_debug:
            if True     increase log output at DEBUG level
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._d_record = d_record
        self._df_social_entity_analysis = df_social_entity_analysis
        self._line_generator = GraphNodeDefGenerator(is_debug=self._is_debug,
                                                     stylesheet_path=stylesheet_path)

        self.state = {"mentions": [], "OpenBy": {}}
        self._build_state()

        if self._is_debug:
            self.logger.debug('\n'.join([
                "CommentNodeBuilder Summary",
                f"\tTotal Mentions: {len(self.state['mentions'])}"]))

    def _nodesize(self,
                  person_name: str) -> dict:
        generator = SocialNodeSizeGenerator(is_debug=self._is_debug,
                                            df=self._df_social_entity_analysis)
        return generator.process(person_name)

    def _build_mentions(self):

        def mentions() -> list:
            return CommentMentionExtractor(d_record=self._d_record,
                                           is_debug=self._is_debug).process()

        for mention in mentions():
            nodesize = self._nodesize(mention)

            node_id = GraphNodeIdGenerator(a_type='person',
                                           a_label=mention).process()

            label = mention.replace('-', ' ')
            label = GraphTextSplitter.split_text(label, threshold=5)

            lines = self._line_generator.process(node_id=node_id,
                                                 node_type='person',
                                                 node_label=label,
                                                 height=nodesize['height'],
                                                 width=nodesize['width'],
                                                 comment=str(self.__class__))

            self.state["mentions"].append({
                "lines": lines,
                "node_id": node_id,
                "person_name": mention})

    def _build_open_by(self):
        parser = CendantRecordParser(is_debug=self._is_debug)
        open_by = parser.field_value_by_name(a_record=self._d_record,
                                             a_field_name='OpenBy')

        nodesize = self._nodesize(open_by)

        node_id = GraphNodeIdGenerator(a_type='person',
                                       a_label=open_by).process()

        label = open_by.replace('-', ' ')
        label = GraphTextSplitter.split_text(label, threshold=5)

        lines = self._line_generator.process(node_id=node_id,
                                             node_type='person',
                                             node_label=label,
                                             height=nodesize['height'],
                                             width=nodesize['width'],
                                             comment=str(self.__class__))

        self.state["OpenBy"] = {
            "lines": lines,
            "node_id": node_id,
            "person_name": open_by}

    def _build_state(self):
        self._build_mentions()
        self._build_open_by()
