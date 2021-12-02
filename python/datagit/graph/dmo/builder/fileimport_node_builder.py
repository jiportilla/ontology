# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from pandas import DataFrame

from base import BaseObject
from datagit.graph.dmo.util import GraphNodeDefGenerator
from datagit.graph.dmo.util import GraphNodeIdGenerator
from datagit.graph.dmo.util import GraphTextSplitter
from dataingest import PythonParseAPI


class FileImportNodeBuilder(BaseObject):
    """ Build a Python File Import Node for Graphviz """

    __df_imports_internal = None

    __df_src = None

    def __init__(self,
                 collection_date: str,
                 stylesheet_path: str,
                 is_debug: bool = True):
        """
        Created:
            24-Dec-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/-cdo/unstructured-analytics/issues/1542#issuecomment-16802985
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
        :param is_debug:
            if True     increase log output at DEBUG level
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._line_generator = GraphNodeDefGenerator(is_debug=self._is_debug,
                                                     stylesheet_path=stylesheet_path)

        api = PythonParseAPI(is_debug=self._is_debug)
        if self.__df_src is None:
            self.__df_src = api.load(date=collection_date).source()

        if self.__df_imports_internal is None:
            self.__df_imports_internal = api.load(date=collection_date).imports().internal()

    @staticmethod
    def _generate_node_id(file_path: str) -> str:
        return GraphNodeIdGenerator(a_type='file_path',
                                    a_label=file_path).process()

    def _generate_lines(self,
                        node_id: str,
                        class_name: str) -> list:

        def _label() -> str:
            label = GraphTextSplitter.split_by_camel_case(
                threshold=10,
                input_text=class_name)
            return label.replace(' ', '_')

        return self._line_generator.process(node_id=node_id,
                                            node_type='file_name',
                                            node_label=_label(),
                                            comment=str(self.__class__.__name__))

    def _target_imports(self,
                        file_path: str) -> list:
        df2 = self.__df_imports_internal[self.__df_imports_internal['SourceFilePath'] == file_path]

        if df2.empty and self._is_debug:
            self.logger.debug('\n'.join([
                "No Internal File Imports Located",
                f"\tSource File: {file_path}"]))
            return []

        target_imports = sorted(df2['TargetFilePath'].unique())

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Located Internal File Imports",
                f"\tSource File: {file_path}",
                f"\tImports (total={len(target_imports)})",
                f"\t\t{target_imports}"]))

        return target_imports

    def process(self,
                df: DataFrame) -> list:
        """
        :param df:
            Sample Input (abridged):
            +-----+--------------+----------------------------------------------------------------------------------------------------------------------------+------------------------------------+
            | AbsolutePath                                                                                                                                    | FileName                           |
            +-------------------------------------------------------------------------------------------------------------------------------------------------+------------------------------------|
            | /Users/craig.trimibm.com/data/workspaces/ibm/text/workspace/cendalytics/cendalytics/skills/core/fcd/skills_report_on_certifications.py | skills_report_on_certifications.py |
            | /Users/craig.trimibm.com/data/workspaces/ibm/text/workspace/cendalytics/cendalytics/skills/core/fcd/skills_report_on_certifications.py | skills_report_on_certifications.py |
            | /Users/craig.trimibm.com/data/workspaces/ibm/text/workspace/cendalytics/cendalytics/skills/core/fcd/skills_report_on_certifications.py | skills_report_on_certifications.py |
            +-----+--------------+----------------------------------------------------------------------------------------------------------------------------+------------------------------------+
        :return:
        """
        results = []

        file_path = sorted(df['FilePath'].unique())[0]
        target_imports = self._target_imports(file_path)

        for target_import in target_imports:
            df2 = self.__df_src[self.__df_src['FilePath'] == target_import]

            class_name = sorted(df2['ClassName'].unique())[0]
            node_id = self._generate_node_id(target_import)

            lines = self._generate_lines(node_id=node_id,
                                         class_name=class_name)

            results.append({
                "lines": lines,
                "node_id": node_id})

        return results
