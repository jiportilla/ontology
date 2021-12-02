# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from datagit.graph.dmo.util import GraphNodeDefGenerator
from datagit.graph.dmo.util import GraphNodeIdGenerator
from datagit.graph.dmo.util import GraphTextSplitter
from datamongo import CendantRecordParser


class CommitNodeBuilder(BaseObject):
    """ Build a GitHub Commit Node for Graphviz """

    # generate mean and stdev via # GIT-1661-16820534
    __commit_total_mean = 6350.46756302521
    __commit_total_stdev = 26842.71482111913

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
            20-Dec-2019
            craig.trim@ibm.com
            *   detect merge commits
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1633#issuecomment-16768132
        Updated:
            25-Dec-2019
            craig.trim@ibm.com
            *   update node line generation
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1654
        Updated:
            26-Dec-2019
            craig.trim@ibm.com
            *   perform dynamic node sizing via
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1661#issuecomment-16820534
        Updated:
            27-Dec-2019
            craig.trim@ibm.com
            *   remove relative sizing
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1669#issue-11256101
        Updated:
            31-Dec-2019
            craig.trim@ibm.com
            *   add cendant-record-parser
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1681#issuecomment-16873873
        Updated:
            31-Dec-2019
            craig.trim@ibm.com
            *   use node line generator to support stylesheet pathing
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1681#issuecomment-16877187
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
        self._line_generator = GraphNodeDefGenerator(is_debug=self._is_debug,
                                                     stylesheet_path=stylesheet_path)

        self.state = {}
        self._build_state()
        self.is_merge_commit = self._is_merge_commit()  # GIT-1633-16768132

    def _is_merge_commit(self):
        """
        Purpose:
            Naive Method to Detect a Merge Commit
        Reference:
            https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1633#issuecomment-16768132
        :return:
            True        the commit record is a merge
            False       not a merge commit
        """
        title = self._parser.field_value_by_name(a_record=self._d_record,
                                                 a_field_name='Title').lower().strip()
        return 'merge' in title

    def _build_state(self):
        def node_id() -> str:
            commit_id = self._d_record['key_field']
            return GraphNodeIdGenerator(a_type='commit',
                                        a_label=commit_id).process()

        def label() -> str:
            additions = self._parser.field_value_by_name(self._d_record, "Additions")
            deletions = self._parser.field_value_by_name(self._d_record, "Deletions")
            total = self._parser.field_value_by_name(self._d_record, "Total")
            title = self._parser.field_value_by_name(self._d_record, "Title")
            title = GraphTextSplitter.split_text(title, threshold=10)

            template = "{{#LABEL}|{Total\\n#TOTAL|{Additions\\n#ADD|{Deletions\\n#DEL}}}}"
            template = template.replace("#LABEL", title)
            template = template.replace("#TOTAL", str(total))
            template = template.replace("#ADD", str(additions))
            template = template.replace("#DEL", str(deletions))

            return template

        label = label()
        node_id = node_id()

        def lines() -> list:
            return self._line_generator.process(node_id=node_id,
                                                node_type='commit',
                                                node_label=label,
                                                comment=str(self.__class__.__name__))

        return {
            "lines": lines(),
            "node_id": node_id}
