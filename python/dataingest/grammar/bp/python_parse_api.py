#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import os

import pandas as pd
from pandas import DataFrame
from tabulate import tabulate

from base import BaseObject


class PythonParseAPI(BaseObject):
    """ API (Orchestrator) for Python Dependency Parsing

    """

    def __init__(self,
                 is_debug: bool = False):
        """
        Created:
            6-Dec-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1535
        Updated:
            24-Dec-2019
            craig.trim@ibm.com
            *   refactored in pursuit of
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1637#issuecomment-16802191
            *   also refactored in pursuit of
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1642#issuecomment-16802836
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)

        self._is_debug = is_debug
        self._base_path = os.environ["CODE_BASE"]

    def _to_file(self,
                 df: DataFrame,
                 collection_type: str) -> None:
        from dataingest.grammar.dmo import CollectionNameGenerator
        from dataingest.grammar.svc import PerformPythonTransformation

        gen_name = CollectionNameGenerator(is_debug=self._is_debug)
        collection_name = gen_name.process(collection_type)

        PerformPythonTransformation(df_parse=df,
                                    is_debug=self._is_debug,
                                    collection_name=collection_name).process()

    def process(self):
        from dataingest.grammar.svc import ParsePythonFiles
        from dataingest.grammar.svc import ParsePythonImports
        from dataingest.grammar.dmo import PythonDirectoryLoader

        directory_path = f"{self._base_path}/workspace"

        files = PythonDirectoryLoader(is_debug=self._is_debug,
                                      directory_path=directory_path).process()

        df_files = ParsePythonFiles(files=files,
                                    is_debug=self._is_debug).process()
        if df_files is None:
            self.logger.warning("No Results Found")
            return

        d_imports = ParsePythonImports(files=files,
                                       df_files=df_files,
                                       is_debug=self._is_debug).process()

        self._to_file(df_files, "src")
        self._to_file(d_imports["internal"], "int-import")
        self._to_file(d_imports["external"], "ext-import")

    def load(self,
             date: str):

        def _input_path(file_name: str) -> str:
            return os.path.join(os.environ["CODE_BASE"],
                                "resources/output/transform",
                                file_name)

        def _to_dataframe(collection_type: str,
                          input_path: str) -> DataFrame:
            df = pd.read_csv(input_path, sep='\t', encoding='utf-8')

            if self._is_debug:
                self.logger.debug('\n'.join([
                    f"Imported {collection_type} Collection",
                    f"\tTotal Records: {len(df)}",
                    f"\tInput Path: {input_path}",
                    tabulate(df.sample(3), tablefmt='psql', headers='keys')]))

            return df

        class Facade(object):

            @staticmethod
            def imports():
                class ImportsFacade(object):
                    @staticmethod
                    def internal() -> DataFrame:
                        input_path = _input_path(f"parse_unstrut-int-import_{date}.csv")
                        return _to_dataframe("Internal Imports", input_path)

                    @staticmethod
                    def external() -> DataFrame:
                        input_path = _input_path(f"parse_unstrut-ext-import_{date}.csv")
                        return _to_dataframe("External Imports", input_path)

                return ImportsFacade()

            @staticmethod
            def source() -> DataFrame:
                input_path = _input_path(f"parse_unstrut-src_{date}.csv")
                return _to_dataframe("Source", input_path)

        return Facade()


def main():
    PythonParseAPI(is_debug=False).process()


if __name__ == "__main__":
    import plac

    plac.call(main)
