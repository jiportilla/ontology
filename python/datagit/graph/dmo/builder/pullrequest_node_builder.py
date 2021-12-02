# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from datagit.graph.dmo.util import GraphNodeDefGenerator
from datagit.graph.dmo.util import GraphNodeIdGenerator


class PullRequestNodeBuilder(BaseObject):
    """ Build a GitHub PullRequest Node for Graphviz """

    def __init__(self,
                 d_record: dict,
                 stylesheet_path: str,
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
            31-Dec-2019
            craig.trim@ibm.com
            *   use node line generator to support stylesheet pathing
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1681#issuecomment-16877187
        :param is_debug:
            if True     increase log output at DEBUG level
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._d_record = d_record

        self._line_generator = GraphNodeDefGenerator(is_debug=self._is_debug,
                                                     stylesheet_path=stylesheet_path)

        self.state = {}
        self._build_state()

    def _build_state(self) -> dict:
        def node_id() -> str:
            pull_request_id = self._d_record['id'].split('-')[-1].strip()
            return GraphNodeIdGenerator(a_label=pull_request_id,
                                        a_type='pull_request').process()

        node_id = node_id()

        def label() -> str:
            return f"PR {node_id}"

        lines = self._line_generator.process(node_id=node_id,
                                             node_type='pull_request',
                                             node_label=label(),
                                             comment=str(self.__class__.__name__))

        return {
            "lines": lines,
            "node_id": node_id}
