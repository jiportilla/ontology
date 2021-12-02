# !/usr/bin/env python
# -*- coding: UTF-8 -*-


import pprint

from base import BaseObject
from base import FileIO


class GraphEdgeStyleFinder(BaseObject):
    """ Utility Methods for Graph Generation """

    def __init__(self,
                 stylesheet_path: str,
                 is_debug: bool = True):
        """
        Created:
            31-Dec-2019
            craig.trim@ibm.com
            *   refactored out of 'node-builder'
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1681#issuecomment-16877316
        :param input_text:
        :param is_debug:
            if True     increase log output at DEBUG level
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._config = FileIO.file_to_yaml_by_relative_path(stylesheet_path)

    def process(self,
                a_type: str):
        a_type = a_type.lower().strip()

        for a_style in self._config['edges']:
            for style in a_style:
                if style.lower().strip() == a_type:
                    return a_style[style]

        self.logger.warning('\n'.join([
            f"Edge Style Not Found (name={a_type})",
            pprint.pformat(self._config['edges'])]))

        raise ValueError
