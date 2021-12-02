# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from pandas import DataFrame

from base import BaseObject
from datagit.graph.dmo.util import GraphNodeDefGenerator
from datagit.graph.dmo.util import GraphNodeIdGenerator
from datagit.graph.dmo.util import GraphTextSplitter


class FileNameNodeBuilder(BaseObject):
    """ Build a Python File Node for Graphviz """

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
        Updated:
            2-Jan-2020
            craig.trim@ibm.com
            *   refactor code into state/build-state standard
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1681#issuecomment-16877328
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

        self.state = {}
        self._build_state()

    def _build_state(self) -> None:
        def node_id() -> str:
            file_name_id = sorted(self._df['FilePath'].unique())[0]
            return GraphNodeIdGenerator(a_type='file_path',
                                        a_label=file_name_id).process()

        def label() -> str:
            class_name = sorted(self._df['ClassName'].unique())[0]
            return GraphTextSplitter.split_by_camel_case(
                threshold=10,
                input_text=class_name)

        label = label()
        node_id = node_id()

        def lines() -> list:
            return self._line_generator.process(node_id=node_id,
                                                node_type='file_name',
                                                node_label=label,
                                                comment=str(self.__class__.__name__))

        self.state = {
            "lines": lines,
            "node_id": node_id}
