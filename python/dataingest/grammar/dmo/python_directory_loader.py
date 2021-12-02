#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import os
from typing import Optional

from base import BaseObject


class PythonDirectoryLoader(BaseObject):
    """ Extract Python File from a Directory Path """

    def __init__(self,
                 directory_path: str,
                 is_debug: bool = False):
        """
        Created:
            24-Dec-2019
            craig.trim@ibm.com
            *   refactored from existing code
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1637#issuecomment-16802191
        :param directory_path:
            a directory path to python code
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)

        self._is_debug = is_debug
        self._directory_path = directory_path

    def _load_files(self) -> list:
        results = []

        for dirpath, dirnames, files in os.walk(self._directory_path):
            for name in files:
                if name.lower().endswith(".py"):
                    results.append(os.path.join(dirpath, name))

        return results

    def process(self) -> Optional[list]:
        files = self._load_files()
        files = [file for file in files if "__init__.py" not in file]

        if self._is_debug and not len(files):
            self.logger.warning('\n'.join([
                "No Files Found",
                f"\tDirectory: {self._directory_path}"]))
            return None

        return files
