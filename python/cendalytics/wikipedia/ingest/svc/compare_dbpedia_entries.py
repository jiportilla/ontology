#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from nlusvc import TextAPI


class CompareDBPediaEntries(BaseObject):
    """ Compare Categories between two dbPedia entities
    """

    _filtered_categories = None

    def __init__(self,
                 entity_name_1: str,
                 entity_name_2: str,
                 ontology_name: str,
                 is_debug: bool = False):
        """
        Created:
            8-Jan-2020
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1710#issuecomment-17017220
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._text_api = TextAPI(is_debug=is_debug,
                                 ontology_name=ontology_name)

        self._entity_name_1 = entity_name_1
        self._entity_name_2 = entity_name_2

    def _find(self,
              entity_name: str) -> dict:
        from cendalytics.wikipedia.ingest.svc import FindDbPediaEntryRedis
        from cendalytics.wikipedia.ingest.svc import PostProcessDBPediaPageRedis

        finder_1 = FindDbPediaEntryRedis(is_debug=self._is_debug)
        entry = finder_1.process(entity_name=entity_name,
                                 ignore_cache=False)

        finder_2 = PostProcessDBPediaPageRedis(is_debug=self._is_debug)
        entry = finder_2.process(entry=entry,
                                 ignore_cache=False)

        return entry

    def _categories(self,
                    entity_name: str) -> set:
        d_entry = self._find(entity_name=entity_name)
        categories = set(d_entry['categories'])

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Entity Resolved",
                f"\tParam Name: {entity_name}",
                f"\tResolved Name: {d_entry['title']}",
                f"\tCategories {categories}"]))

        return categories

    def process(self) -> list:
        categories_1 = self._categories(self._entity_name_1)
        categories_2 = self._categories(self._entity_name_2)

        common = sorted(categories_1.intersection(categories_2))

        self.logger.debug('\n'.join([
            "Entity Comparison Completed",
            f"\tEntity 1: {self._entity_name_1}",
            f"\tEntity 2: {self._entity_name_2}",
            f"\tCommon Entities (total={len(common)})",
            f"\t\t{common}"]))

        return common
