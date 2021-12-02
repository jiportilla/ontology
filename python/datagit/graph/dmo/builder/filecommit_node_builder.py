# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from datagit.graph.dmo.util import GraphNodeDefGenerator
from datagit.graph.dmo.util import GraphNodeIdGenerator
from datagit.graph.dmo.util import GraphTextSplitter
from datamongo import CendantRecordParser


class FileCommitNodeBuilder(BaseObject):
    """ Build a GitHub File Commit Node for Graphviz """

    # generate mean and stdev via # GIT-1661-16820539
    __filecommit_total_mean = 766.4357403651115
    __filecommit_total_stdev = 8257.509657898225 / 2  # no negative numbers; compensate on distribution

    __default_label_size = 7.00
    __zscore_multiplier = 3.50

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
            26-Dec-2019
            craig.trim@ibm.com
            *   perform dynamic node sizing via
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1661#issuecomment-16820534
        Updated:
            27-Dec-2019
            craig.trim@ibm.com
            *   dynamic font size (ref 'relative sizing on labels')
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1669#issue-11256101
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

        self.changes = self._load_changes(d_record)
        self.fontsize = self._load_fontsize()

        self.file_name = self._generate_file_name(d_record)

        self.state = {}
        self._build_state()

    def _generate_file_name(self,
                            d_record: dict) -> str:
        file_name = self._parser.field_value_by_name(d_record, 'file')
        file_name = file_name.split('/')[-1]

        return file_name

    def _load_changes(self,
                      d_record: dict) -> str:
        return str(self._parser.field_value_by_name(d_record, 'changes'))

    def _load_fontsize(self) -> float:
        z = (float(self.changes) - self.__filecommit_total_mean) / self.__filecommit_total_stdev

        total = self.__default_label_size + round(z * self.__zscore_multiplier, 1)
        return round(total, 0)

    def _build_state(self):
        def node_id() -> str:
            file_commit_id = self._d_record['key_field']
            return GraphNodeIdGenerator(a_type='file_commit',
                                        a_label=file_commit_id).process()

        node_id = node_id()

        def lines() -> list:
            label = GraphTextSplitter.split_text(self.file_name)

            return self._line_generator.process(node_id=node_id,
                                                node_type='file_commit',
                                                node_label=label,
                                                comment=str(self.__class__.__name__))

        return {
            "lines": lines,
            "node_id": node_id}
