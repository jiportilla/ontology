#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import os
import pprint

from base import BaseObject
from base import FileIO


class PythonImportParser(BaseObject):
    """ Extract imports from a Python file

    Sample Input:
        import sys
        from requests import Session
        from base import BaseObject
        from base import DataTypeError
        from dataingest.github.svc import LoadGitHubData
        from dataingest.github.dmo import GitHubSessionAuthenticator
        from dataingest.github.dmo import GitHubURLGenerator
        from dataingest.github.svc import ExtractGitHubData
        from dataingest.github.svc import TransformGitHubStructure

    Intermediate Output:
        {   'base': {
                'DataTypeError',
                'BaseObject'},
            'dataingest.github.dmo': {
                'GitHubURLGenerator',
                'GitHubSessionAuthenticator'},
            'dataingest.github.svc': {
                'ExtractGitHubData',
                'LoadGitHubData',
                'TransformGitHubStructure'},
            'requests': {
                'Session'},
            'sys': set()
        }

    Final Output:
        +----+---------------------+---------------+--------+------+
        |    | Import              | L0            | L1     | L2   |
        |----+---------------------+---------------+--------+------|
        |  0 | Session             | requests      | nan    | nan  |
        |  1 | DataTypeError       | base       | nan    | nan  |
        |  2 | BaseObject          | base       | nan    | nan  |
        |  3 | LoadGitHubData      | dataingest | github | svc  |
        |  4 | TransformGitHubStructure | dataingest | github | svc  |
        |  5 | ExtractGitHubData   | dataingest | github | svc  |
        |  6 | GitHubSessionAuthenticator | dataingest | github | dmo  |
        |  7 | GitHubURLGenerator  | dataingest | github | dmo  |
        +----+---------------------+---------------+--------+------+
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
            *   renamed from 'python-code-parser'
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1637#issuecomment-16802139
        :param file_path:
            link to a python file
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)

        self._is_debug = is_debug
        self._file_path = file_path

    @staticmethod
    def _extract_imports(lines: list) -> dict:
        """
        Purpose:
            Extract package imports into a dictionary structure
        Sample Input:
            from base import BaseObject
            from base import FileIO
        Sample Output:
            {'base': {'FileIO', 'BaseObject'}}
        :param lines:
        :return:
            a dictionary of package imports
        """
        d = {}
        for line in lines:
            if " import " in line and "from " in line:

                tokens = line.split(' ')
                if len(tokens) != 4:
                    continue

                pkg = tokens[1]
                if pkg not in d:
                    d[pkg] = set()
                [d[pkg].add(x.strip()) for x in tokens[3].split(',') if x and len(x)]

            elif line.startswith("import "):
                tokens = sorted(set(line.split(" ")[1:]))
                for token in tokens:
                    if token not in d:
                        d[token] = set()

        return d

    def _relative_path(self):
        base_path = os.environ["CODE_BASE"]
        if base_path not in self._file_path:
            raise NotImplementedError
        return self._file_path.split(base_path)[-1][1:].strip()

    def _to_list(self,
                 d_dependencies: dict) -> list:
        results = []

        for k in d_dependencies:
            for v in d_dependencies[k]:

                d = {"Import": v,
                     "Provenance": str(self.__class__.__name__),
                     "RelativePath": self._relative_path(),
                     "FilePath": self._file_path}

                tokens = [x for x in k.split('.') if x and len(x)]

                ctr = 0
                for token in tokens:
                    d[f"L{ctr}"] = token
                    ctr += 1

                results.append(d)

        return results

    def process(self) -> list:
        lines = FileIO.file_to_lines(self._file_path, use_sort=False)

        d_dependencies = self._extract_imports(lines)
        results = self._to_list(d_dependencies)

        if self._is_debug and len(results):
            def _sample() -> list:
                if len(results) > 3:
                    return results[:3]
                return results

            self.logger.debug('\n'.join([
                f"Code Parsing Complete (total-records={len(results)})",
                pprint.pformat(_sample(), indent=4)]))

        return results
