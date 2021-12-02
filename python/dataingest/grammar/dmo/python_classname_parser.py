#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pprint
from typing import Optional

from base import BaseObject
from base import FileIO


class PythonClassNameParser(BaseObject):
    """ Parse Class Name(s) from a Python File """

    def __init__(self,
                 file_path: str,
                 is_debug: bool = False):
        """
        Created:
            24-Dec-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1637
        :param file_path:
            link to a python file
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)

        self._is_debug = is_debug
        self._file_path = file_path

    @staticmethod
    def _extract(a_line: str) -> Optional[str]:
        """
        Purpose:
            Extract Class Name from a line of code
        :param a_line:
            Sample Input:
                class PythonDirectoryParser(BaseObject):
        :return:
            Sample Output:
                PythonDirectoryParser
        """
        return a_line.split('class')[-1].split('(')[0].strip()

    def _lines(self) -> list:
        lines = FileIO.file_to_lines(self._file_path, use_sort=False)
        lines = [line for line in lines if line.startswith('class ')]

        return lines

    def _names(self,
               lines: list) -> list:
        names = [self._extract(line) for line in lines]
        names = [name for name in names if 'facade' not in name.lower()]

        return names

    def _disambiguate(self,
                      names: list) -> str:
        """
        Purpose:
            Disambiguate in the event of multiple namnes
        Sample Input:
            [   'KeyTerm',
                'TextacyKeytermRanking' ]
        Given FilePath:
            '/Users/craig.trimibm.com/data/workspaces/ibm/text/workspace/nlusvc/nlusvc/textacy/dmo/textacy_keyterm_ranking.py'
        Infer FileName:
            'textacy_keyterm_ranking.py'
        Infer Match:
            'textacy_keyterm_ranking.py' => 'textacy_keyterm_ranking' => 'textacykeytermranking'
        Sample Output:
             'TextacyKeytermRanking'
        :param names:
            a list of candidate class names
        :return:
            the first most likely class name
        """
        file_name = self._file_path.split('/')[-1].split('.')[0].strip().replace('_', '').lower()
        matching_names = [name for name in names if name.lower() == file_name]

        if len(matching_names):
            return matching_names[0]

        return names[0]

    def _to_dict(self,
                 names: list) -> dict:

        def _class_name() -> str:
            if len(names) > 1:
                name = self._disambiguate(names)

                # if self._is_debug:
                self.logger.info('\n'.join([
                    "Class Name Disambiguation Complete",
                    f"\tOriginal: {names}",
                    f"\tNormalize: {name}",
                    f"\tFile Path: {self._file_path}"]))

                return name
            return names[0]

        return {
            "Provenance": str(self.__class__.__name__),
            "FilePath": self._file_path,
            "ClassName": _class_name()}

    def process(self) -> Optional[dict]:

        lines = self._lines()
        if not lines:
            return None

        names = self._names(lines)
        if not names:
            return None

        d_result = self._to_dict(names)

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Class Name Extraction Complete",
                pprint.pformat(d_result, indent=4)]))

        return d_result
