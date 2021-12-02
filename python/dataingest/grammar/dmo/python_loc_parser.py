#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pprint

from base import BaseObject
from base import FileIO


class PythonLOCParser(BaseObject):
    """ Parse T/LOC from a Python File

    """

    def __init__(self,
                 file_path: str,
                 is_debug: bool = False):
        """
        Created:
            24-Dec-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1637#issuecomment-16802191
        :param file_path:
            link to a python file
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)

        self._is_debug = is_debug
        self._file_path = file_path

    def _lines(self) -> list:
        lines = FileIO.file_to_lines(self._file_path, use_sort=False)

        return lines

    def process(self) -> dict:
        lines = self._lines()

        loc = len(lines)
        tloc = len([line for line in lines if line and len(line.strip())])

        d_result = {
            "Provenance": str(self.__class__.__name__),
            "FilePath": self._file_path,
            "LOC": str(loc),
            "TLOC": str(tloc)}

        if self._is_debug:
            self.logger.debug('\n'.join([
                "LOC Parsing Complete",
                pprint.pformat(d_result, indent=4)]))

        return d_result
