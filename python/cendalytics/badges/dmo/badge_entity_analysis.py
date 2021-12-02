#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from typing import Dict

from base import BaseObject
from nlutext.core.bp import TextParser


class BadgeEntityAnalysis(BaseObject):
    """
    Generate a sorted array of cendant tags and weights
    from the badge name and the supplied ingested tags
    """

    __cache:Dict[str,list] = {}
    __parser = None

    def __init__(self,
                 badge_name: str,
                 raw_tags: list,
                 is_debug: bool = False):
        """
        Created:
            19-Apr-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/97
        Updated:
            15-Aug-2019
            craig.trim@ibm.com
            *   add mongo host as a param; driven by
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/767
        Updated:
            22-Aug-2019
            craig.trim@ibm.com
            *   remove text-parser caching
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/796#issuecomment-14041159
        Updated:
            12-Jan-2020
            xavier.verges@es.ibm.com
            *   changed the interface to get badge name + array of tags and
                return a sorted array of (tag, weight)
            *   keep TextParser cached
        """
        BaseObject.__init__(self, __name__)
        self._text_snipnets = raw_tags + [badge_name]
        self._is_debug = is_debug

    @classmethod
    def _parse(cls,
               some_tag: str,
               is_debug: bool) -> list:
        if some_tag in cls.__cache:
            return cls.__cache[some_tag]
        if not cls.__parser:
            cls.__parser = TextParser(is_debug=is_debug)
        cls.__cache[some_tag] = cls.__parser.process(some_tag)['tags']['supervised']
        return cls.__cache[some_tag]

    def process(self) -> list:

        tag_weights:Dict[str, float] = {}

        for snipnet in self._text_snipnets:
            tags = self._parse(snipnet, self._is_debug)
            for tag, weight in tags:
                if not tag in tag_weights:
                    tag_weights[tag] = -1
                if weight > tag_weights[tag]:
                    tag_weights[tag] = weight

        def sort_by_key(tag_weight_tuple):
            return tag_weight_tuple[1]

        return sorted(tag_weights.items(), key=sort_by_key, reverse=True)
