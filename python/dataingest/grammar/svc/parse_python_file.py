#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pprint

from base import BaseObject


class ParsePythonFile(BaseObject):
    """ Parse a single Python Code File """

    def __init__(self,
                 file_path: str,
                 is_debug: bool = False):
        """
        Created:
            24-Dec-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1637#issuecomment-16802139
        :param file_path:
            a path to a single Python file
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)

        self._is_debug = is_debug
        self._file_path = file_path

    def _merge(self,
               d_loc: dict,
               d_classname: dict,
               d_path_segmentation: dict) -> dict:

        def _classname() -> str:
            if d_classname:
                return d_classname['ClassName']
            return d_path_segmentation['FileName'].split('.')[0].strip()

        d_result = {
            'LOC': d_loc['LOC'],
            'TLOC': d_loc['TLOC'],
            'ClassName': _classname(),
            'FilePath': self._file_path,
            'SOAType': d_path_segmentation['SOAType'],
            'FileName': d_path_segmentation['FileName'],
            'RelativePath': d_path_segmentation['RelativePath']}

        keys = [k for k in d_path_segmentation.keys() if k.startswith('D')]
        for key in keys:
            d_result[key] = d_path_segmentation[key]

        return d_result

    def process(self) -> list:
        from dataingest.grammar.dmo import PythonLOCParser
        from dataingest.grammar.dmo import PythonClassNameParser
        from dataingest.grammar.dmo import PythonPathSegmentation

        results = []

        d_classname = PythonClassNameParser(is_debug=self._is_debug,
                                            file_path=self._file_path).process()

        d_loc = PythonLOCParser(is_debug=self._is_debug,
                                file_path=self._file_path).process()

        d_path_segmentation = PythonPathSegmentation(is_debug=self._is_debug,
                                                     file_path=self._file_path).process()

        results.append(self._merge(d_classname=d_classname,
                                   d_loc=d_loc,
                                   d_path_segmentation=d_path_segmentation))

        if self._is_debug:
            self.logger.warning('\n'.join([
                "Python Parsing Complete",
                pprint.pformat(results, indent=4)]))

        return results
