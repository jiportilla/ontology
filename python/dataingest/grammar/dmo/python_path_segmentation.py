#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import os
import pprint

from base import BaseObject


class PythonPathSegmentation(BaseObject):
    """ Perform SOA (Service Oriented Architecture) analysis on a Python File

    Sample Output:
    {   'D0': 'workspace',
        'D1': 'dataingest',
        'D2': 'dataingest',
        'D3': 'grammar',
        'D4': 'svc',
        'FileName': 'parse_python_file.py',
        'FilePath': '/Users/craig.trimibm.com/data/workspaces/ibm/text/workspace/dataingest/dataingest/grammar/svc/parse_python_file.py',
        'Provenance': 'PythonPathSegmentation',
        'RelativePath': 'workspace/dataingest/dataingest/grammar/svc/parse_python_file.py',
        'SOAType': 'Service' }
    """

    def __init__(self,
                 file_path: str,
                 is_debug: bool = False):
        """
        Created:
            3-Dec-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1521
        Updated:
            24-Dec-2019
            craig.trim@ibm.com
            *   refactored out of 'python-directory-parser'
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1637#issuecomment-16802191
        :param file_path:
            link to a python file
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)

        self._is_debug = is_debug
        self._file_path = file_path
        self._base_path = os.environ["CODE_BASE"]

    @staticmethod
    def soa_type(paths: list) -> str:
        if "svc" in paths:
            return "Service"
        if "dmo" in paths:
            return "Domain Component"
        if "dto" in paths:
            return "Domain Transfer Object"
        if "bp" in paths:
            return "Business Process"
        if "os" in paths:
            return "Data File"
        return "Other"

    def _relative_path(self) -> str:
        if self._base_path not in self._file_path:
            raise NotImplementedError
        return self._file_path.split(self._base_path)[-1][1:].strip()

    def _parse_file(self) -> dict:

        def file_name() -> str:
            return self._file_path.split("/")[-1].strip()

        file_name = file_name()
        relative_path = self._relative_path()

        paths = [x for x in relative_path.split(file_name)[0].strip().split('/')
                 if x and len(x)]

        d = {
            "Provenance": str(self.__class__.__name__),
            "FileName": file_name,
            "SOAType": self.soa_type(paths),
            "FilePath": self._file_path,
            "RelativePath": relative_path}

        ctr = 0
        for path in paths:
            d[f"D{ctr}"] = path
            ctr += 1

        return d

    def process(self) -> dict:
        d_result = self._parse_file()

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Path Segmentation Complete",
                pprint.pformat(d_result, indent=4)]))

        return d_result
