#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from datadict import FindEntity
from datadict import FindRelationships


class EntityConceptResolution(BaseObject):
    """ Resolve an incoming list of Concepts (tags) """

    def __init__(self,
                 tags: list,
                 ontology_name: str = 'base',
                 is_debug: bool = False):
        """
        Created:
            25-Nov-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1444
        Updated:
            13-Dec-2019
            craig.trim@ibm.com
            *   load dictionaries by ontology name
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1582
        :param tags:
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)

        self._tags = tags
        self._is_debug = is_debug
        self._rel_finder = FindRelationships(is_debug=is_debug,
                                             ontology_name=ontology_name)
        self._entity_finder = FindEntity(is_debug=self._is_debug,
                                         ontology_name=ontology_name)

    def resolve(self,
                tags: list) -> list:
        master = set()

        for tag in tags:
            resolutions = self._rel_finder.see_also(some_token=tag)
            if not resolutions or not len(resolutions):
                master.add(tag)
            else:
                [master.add(x) for x in resolutions]

        return sorted([tag for tag in master if tag and len(tag)])

    def cleanse(self,
                tags: list) -> list:
        return sorted(set([self._entity_finder.label_or_self(x) for x in tags]))

    def process(self) -> list:
        resolved = self.cleanse(self.resolve(self._tags))

        if self._is_debug and resolved != self._tags:
            self.logger.debug('\n'.join([
                "Tag Resolution Complete",
                f"\tOriginal: {self._tags}",
                f"\tResolved: {resolved}"]))

        return resolved
