#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import os
import pprint

from pandas import DataFrame

from base import BaseObject
from base import FileIO


class GenerateDimDictionaries(BaseObject):
    """ Generate Dimemsionality Dictionaries

        Dimensionailty Configuration is performed within a YML file
            with arbitrary and nested complexity

        This service takes the dimensionality configuration files
            and generates flat DataFrames that are saved to python dictionary structures

        This reduces runtime complexity
    """

    def __init__(self,
                 is_debug: bool = False):
        """
        Created:
            29-Oct-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/916#issuecomment-15618121
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug

    def _files(self) -> list:
        path = os.path.join(os.environ['CODE_BASE'], 'resources/config/dimensionality')
        files = FileIO.load_files(path, "yml")
        self.logger.debug('\n'.join([
            f"Loaded Files (total={len(files)})",
            f"\tPath: {path}"]))

        return files

    def _dataframes(self) -> dict:
        from taskmda.mda.dmo import DimSchemaNormalization

        d = {}
        for file in self._files():

            d_schema = FileIO.file_to_yaml(file)
            df = DimSchemaNormalization(d_schema, is_debug=self._is_debug).process()

            filename = file.split('/')[-1].split('.')[0].strip()
            if self._is_debug:
                self.logger.debug(f"Loaded DataFrame ("
                                  f"size={len(df)}, "
                                  f"name={filename})")

            d[filename] = df

        return d

    @staticmethod
    def _to_dict(a_dataframe: DataFrame) -> dict:
        d = {}
        for _, row in a_dataframe.iterrows():
            if row['Parent'] not in d:
                d[row['Parent']] = []
            d[row['Parent']].append(row['Child'])

        return d

    def _generate_file(self,
                       a_name: str,
                       d_dim: dict):
        from taskmda.mda.dto import KbNames
        from taskmda.mda.dto import KbPaths
        from taskmda.mda.dmo import GenericTemplateAccess

        the_json_result = pprint.pformat(d_dim, indent=4)
        the_json_result = "{0} = {{\n {1}".format(
            KbNames.dimesionality(a_name), the_json_result[1:])

        the_template_result = GenericTemplateAccess.process()
        the_template_result = the_template_result.replace(
            "CONTENT", the_json_result)

        path = os.path.join(os.environ["CODE_BASE"],
                            KbPaths.dimesionality(a_name))
        FileIO.text_to_file(path, the_template_result)

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Wrote to File",
                f"\tPath: {path}",
                f"\tName: {a_name}",
                f"\tSize: {len(d_dim)}"]))

    def process(self):
        d_results = self._dataframes()
        for filename in d_results:
            df = d_results[filename]
            self._generate_file(a_name=filename,
                                d_dim=self._to_dict(df))
