# !/usr/bin/env python
# -*- coding: UTF-8 -*-


import pprint

from base import BaseObject
from base import FileIO


class GraphNodeDefGenerator(BaseObject):
    """ Build GitHub Node Definition for Graphviz """

    def __init__(self,
                 stylesheet_path: str,
                 is_debug: bool = True):
        """
        Created:
            31-Dec-2019
            craig.trim@ibm.com
            *   refactored out of 'node-builder'
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1681#issuecomment-16877187
            *   renamed from 'node-line-generator'
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1681#issuecomment-16877277
        :param is_debug:
            if True     increase log output at DEBUG level
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._config = FileIO.file_to_yaml_by_relative_path(stylesheet_path)

    @staticmethod
    def _cleanse_node_label(node_label: str) -> str:
        """
        Purpose:
            Provide generic node label cleaning for Graphviz
        :param node_label:
            a node label
        :return:
            a cleansed node label
        """
        node_label = node_label.replace('"', '')  # GIT-1651-16811213

        return node_label

    def process(self,
                node_id: str,
                node_type: str,
                node_label: str,
                comment: str,
                **kwargs) -> list:
        """
        :param node_id:
            the Unique Node identifier
        :param node_type:
            the type of Node
            e.g., 'issue', 'commit', 'pull-request'
        :param node_label:
            the display label for the Node
        :param comment:
            a docstring comment for GraphViz
        :return:
            the lines necessary to render a Graphviz Node
        """
        node_type = node_type.lower().strip()

        def _node_style() -> dict:
            """
            :return:
                the Node Styling block from the Graphviz Stylesheet definition
            """
            for a_style in self._config['nodes']:
                for style in a_style:
                    if style.lower().strip() == node_type:
                        return a_style[style]

            self.logger.warning('\n'.join([
                f"Node Style Not Found (name={node_type})",
                pprint.pformat(self._config['nodes'])]))

            raise ValueError

        d_node_style = _node_style()

        lines = [
            f"# {comment}",
            f"{node_id}",
            "["]

        def _display_label() -> bool:
            if 'show_label' not in d_node_style:
                return True
            return bool(d_node_style['show_label'])

        def _is_valid_key(a_key: str) -> bool:
            if a_key == 'label' and not _display_label():
                return False
            return True

        explicit_keys = d_node_style.keys()
        explicit_keys = [key for key in explicit_keys if key not in kwargs]
        explicit_keys = [key for key in explicit_keys if _is_valid_key(key)]

        for k in explicit_keys:  # explicit styles via stylesheet
            value = d_node_style[k]
            lines.append(f"\t{k}=\"{value}\"")

        kwarg_keys = kwargs.keys()
        kwarg_keys = [key for key in kwarg_keys if _is_valid_key(key)]

        for k in kwarg_keys:  # dynamic styles via param
            value = kwargs[k]
            lines.append(f"\t{k}=\"{value}\"")

        def label() -> str:
            if 'label' in d_node_style:
                return d_node_style['label']
            return self._cleanse_node_label(node_label)

        lines.append(f"\tlabel=\"{label()}\"")
        lines.append("]\n")

        return lines
