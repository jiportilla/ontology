# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject


class NodeStyleMatcher(BaseObject):
    """
    Purpose:
    Find a matching node style from a Graphviz Stylesheet

    Traceability:
    https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/240
    """

    def __init__(self,
                 graph_style: dict,
                 is_debug: bool = True):
        """
        Created:
            10-May-2019
            craig.trim@ibm.com
            *   refactored out of 'generate-inference-graph'
        :param is_debug:
            if True     increase log output at DEBUG level
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._graph_style = graph_style

    def default_node_style(self) -> dict:
        """Set Default Graphviz Node Styles """

        for node in self._graph_style["nodes"]:
            d_node = node["node"]
            if "conditions" not in d_node:
                return d_node["style"]

        raise NotImplementedError("\n".join([
            "Default Node Style Undefined"]))

    def process(self,
                a_tag_type: str,
                is_variant: bool,
                is_primary: bool) -> dict:
        for node in self._graph_style["nodes"]:
            d_node = node["node"]

            if "conditions" in d_node:
                d_conditions = d_node["conditions"]

                def _matches_variant():
                    if "is_variant" not in d_conditions:
                        return True
                    return d_conditions["is_variant"] == is_variant

                def _matches_primary():
                    if "is_primary" not in d_conditions:
                        return True
                    return d_conditions["is_primary"] == is_primary

                def _matches_type():
                    if "type" not in d_conditions:
                        return True
                    return d_conditions["type"].lower() == a_tag_type.lower()

                if _matches_primary() and _matches_variant() and _matches_type():
                    if "style" not in d_node:
                        raise NotImplementedError("\n".join([
                            "Matching Node Style Undefined"]))

                    return d_node["style"]

        return self.default_node_style()
