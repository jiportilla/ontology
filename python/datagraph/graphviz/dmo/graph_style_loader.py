# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from base import FileIO


class GraphStyleLoader(BaseObject):
    """
    Purpose:
    Load a Graphviz Stylesheet

    Traceability:
    https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1426#issuecomment-16165027

    Prereq:
    """

    def __init__(self,
                 style_name: str = "nlp",
                 is_debug: bool = True):
        """
        Created:
            21-Nov-2019
            craig.trim@ibm.com
        :param style_name:
            the name of the graph stylesheet to use
        :param is_debug:
            if True     increase log output at DEBUG level
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._style_name = self._load(style_name)

    def style(self) -> dict:
        return self._style_name

    @staticmethod
    def _load(some_style_name) -> dict:
        def _relative_path():
            if "nlp" in some_style_name.lower():
                return "resources/config/graph/graphviz_nlp_graph.yml"
            if "big" in some_style_name.lower():
                return "resources/config/graph/graphviz_big_graph.yml"
            if "sentiment" in some_style_name.lower():
                return "resources/config/graph/graphviz_sentiment_graph.yml"
            raise NotImplementedError

        return FileIO.file_to_yaml_by_relative_path(
            _relative_path())
