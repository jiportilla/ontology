# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from pandas import DataFrame
from pandas import Series

from base import BaseObject
from datagit.graph.dmo.util import GraphNodeDefGenerator
from datagit.graph.dmo.util import GraphNodeIdGenerator


class FileFolderNodeBuilder(BaseObject):
    """ Build a Python File Folder for Graphviz """

    def __init__(self,
                 df: DataFrame,
                 stylesheet_path: str,
                 is_debug: bool = True):
        """
        Created:
            19-Dec-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1631
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
            *   refactor node-id
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1681#issuecomment-16877316
        :param df:
            +------+-----------+---------------+---------------+------+------+------+----------------+-------------------+---------------+------+------+------+------------------+
            |      | D0        | D1            | D2            | D3   | D4   |   D5 | FileName       | Import            | L0            | L1   | L2   |   L3 | SOAType          |
            |------+-----------+---------------+---------------+------+------+------+----------------+-------------------+---------------+------+------+------+------------------|
            | 1036 | workspace | dataingest | dataingest | push | bp   |  nan | persist_api.py | BaseObject        | base       | nan  | nan  |  nan | Business Process |
            | 1037 | workspace | dataingest | dataingest | push | bp   |  nan | persist_api.py | BaseMongoClient   | datamongo  | nan  | nan  |  nan | Business Process |
            | 1038 | workspace | dataingest | dataingest | push | bp   |  nan | persist_api.py | PersistDatatoDB   | dataingest | push | dmo  |  nan | Business Process |
            | 1039 | workspace | dataingest | dataingest | push | bp   |  nan | persist_api.py | DBCredentialsAPI  | datadb2    | core | bp   |  nan | Business Process |
            | 1040 | workspace | dataingest | dataingest | push | bp   |  nan | persist_api.py | PushTagCollection | dataingest | push | svc  |  nan | Business Process |
            | 1041 | workspace | dataingest | dataingest | push | bp   |  nan | persist_api.py | PushXdmCollection | dataingest | push | svc  |  nan | Business Process |
            +------+-----------+---------------+---------------+------+------+------+----------------+-------------------+---------------+------+------+------+------------------+
        :param is_debug:
            if True     increase log output at DEBUG level
        """
        BaseObject.__init__(self, __name__)
        self._df = df
        self._is_debug = is_debug
        self._line_generator = GraphNodeDefGenerator(is_debug=self._is_debug,
                                                     stylesheet_path=stylesheet_path)

        self.lines = []
        self.triples = []
        self.touchpoint_node_id = None

        self.state = []
        self._generate()

    @staticmethod
    def _generate_node_id(input_text: str) -> str:
        return GraphNodeIdGenerator(a_type='file_folder',
                                    a_label=input_text).process()

    def _generate(self):

        def _first_row() -> Series:
            for _, row in self._df.iterrows():
                return row

        first_row = _first_row()

        columns = ['D0', 'D1', 'D2', 'D3', 'D4', 'D5']
        columns = [column for column in columns
                   if type(first_row[column]) == str]

        buffer = []
        prior_node_id = None

        for column in columns:
            buffer.append(first_row[column])

            node_id = self._generate_node_id('_'.join(buffer))
            node_lines = self._line_generator.process(node_id=node_id,
                                                      node_type='file_folder',
                                                      node_label=buffer[-1],
                                                      comment=str(self.__class__.__name__))

            self.lines.append(node_lines)

            if prior_node_id is not None:
                self.triples.append({
                    "subject": prior_node_id,
                    "predicate": "composes",
                    "object": node_id})

            prior_node_id = node_id

            # touchpoint will always be the last node in the sequence
            self.touchpoint_node_id = node_id
