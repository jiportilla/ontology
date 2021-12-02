# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from typing import Optional

from pandas import DataFrame

from base import BaseObject


class GraphEdgeDefGenerator(BaseObject):
    """ Build and Style all the Graphviz Edges """

    __default_keys = ["subject", "predicate", "object", "label"]

    def __init__(self,
                 triples: list,
                 stylesheet_path: str,
                 df_social_rel_analysis: Optional[DataFrame],
                 is_debug: bool = True):
        """
        Created:
            24-Dec-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1645
        Updated:
            26-Dec-2019
            craig.trim@ibm.com
            *   dynamic label capability
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1648#issuecomment-16817744
            *   edge style attributes derived entirely from stylesheet
                https://github.ibm.com/-cdo/unstructured-analytics/issues/1657
        Updated:
            27-Dec-2019
            craig.trim@ibm.com
            *   refactor into kwargs
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1669#issue-11256101
        Updated:
            31-Dec-2019
            craig.trim@ibm.com
            *   renamed from 'graph-edge-builder' in pursuit of
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1681#issuecomment-16877277
            *   use stylesheet-pathing as param
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1684
        Updated:
            2-Jan-2020
            craig.trim@ibm.com
            *   import df-social-rel-analysis as param
                https://github.ibm.com/-cdo/unstructured-analytics/issues/1680#issuecomment-16901723
        :param is_debug:
            if True     increase log output at DEBUG level
        """
        BaseObject.__init__(self, __name__)
        from datagit.graph.dmo.util import GraphEdgeStyleFinder

        self._triples = triples
        self._is_debug = is_debug
        self._df_social_rel_analysis = df_social_rel_analysis
        self._edge_style_finder = GraphEdgeStyleFinder(is_debug=self._is_debug,
                                                       stylesheet_path=stylesheet_path)

    def _weight(self,
                person_a: str,
                person_b: str) -> float:
        from datagit.graph.dmo.util import SocialRelWeightGenerator

        generator = SocialRelWeightGenerator(is_debug=self._is_debug,
                                             df=self._df_social_rel_analysis)
        return generator.social_collocation(person_a, person_b)

    def process(self) -> list:
        lines = []

        for triple in self._triples:

            d_edge_style = self._edge_style_finder.process(triple['predicate'])
            existing_keys = set(self.__default_keys)

            lines.append(f"\t{triple['subject']} -> {triple['object']}")
            lines.append(f"[")

            # Add Label (Optional)
            def _label() -> Optional[str]:
                display_label = bool(d_edge_style["display_label"])
                has_label = 'label' in triple and triple['label'] is not None

                if display_label:
                    if has_label:  # User Defined Label
                        return f"\tlabel=\"{triple['label']}\""

                    # Predicate Label
                    return f"\tlabel=\"{triple['predicate']}\""

                # No Label

            lines.append(_label())

            if 'subject_name' in triple and 'object_name' in triple:
                weight = self._weight(triple['subject_name'], triple['object_name'])
                weight = 1.0 + weight
                lines.append(f"\tweight=\"{weight}\"")

            # Add User Keys (Overrides Stylesheet)
            keys = [key for key in triple.keys()
                    if key not in existing_keys]
            for key in keys:
                existing_keys.add(key)
                lines.append(f"\t{key}=\"{triple[key]}\"")

            # Add Stylesheet Keys
            style_keys = [key for key in d_edge_style.keys()
                          if key not in existing_keys]
            for key in style_keys:
                lines.append(f"\t{key}=\"{d_edge_style[key]}\"")

            lines.append("]\n")

        lines = [line for line in lines
                 if line and len(line.strip())]
        return lines
