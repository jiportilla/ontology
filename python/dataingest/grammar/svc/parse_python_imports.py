#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import os

import pandas as pd
from pandas import DataFrame
from tabulate import tabulate

from base import BaseObject


class ParsePythonImports(BaseObject):
    """ Extract and Augment Imports from Python Files  """

    def __init__(self,
                 files: list,
                 df_files: DataFrame,
                 is_debug: bool = False):
        """
        Created:
            24-Dec-2019
            craig.trim@ibm.com
            *   refactored and augmented from existing code
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1642
        :param files:
            a list of paths to python files in a workspace
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)

        self._files = files
        self._df_files = df_files
        self._is_debug = is_debug
        self._base_path = os.environ["CODE_BASE"]

    def _log(self,
             import_type: str,
             df_imports: DataFrame) -> None:
        def _sample() -> DataFrame:
            if len(df_imports) > 3:
                return df_imports.sample(3)
            return df_imports

        self.logger.debug('\n'.join([
            f"{import_type} Imports Created",
            tabulate(_sample(), headers='keys', tablefmt='psql')]))

    def _generate(self):
        from dataingest.grammar.dmo import PythonImportParser

        external_imports = []  # a class imports a third-party component
        internal_imports = []  # a class imports another component within the workspace

        for file in self._files:
            imports = PythonImportParser(file_path=file,
                                         is_debug=self._is_debug).process()

            for an_import in imports:
                import_classname = an_import['Import']
                df2 = self._df_files[self._df_files['ClassName'] == import_classname]
                if df2.empty:
                    d_external = {
                        "FilePath": file,
                        "Import": import_classname}
                    if 'L0' in an_import:
                        d_external["From"] = an_import['L0']
                    external_imports.append(d_external)
                else:
                    for _, row in df2.iterrows():
                        internal_imports.append({
                            "SourceFilePath": file,
                            "TargetFilePath": row['FilePath']})

        return pd.DataFrame(external_imports), pd.DataFrame(internal_imports)

    def process(self) -> dict:
        df_external_imports, df_internal_imports = self._generate()

        if self._is_debug:
            if not df_external_imports.empty:
                self._log("External", df_external_imports)
            if not df_internal_imports.empty:
                self._log("Internal", df_internal_imports)

        return {
            "external": df_external_imports,
            "internal": df_internal_imports}
