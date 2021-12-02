#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pprint
from collections import Counter

from base import BaseObject
from base import MandatoryParamError


class GenerateMetrics(BaseObject):
    """ """

    def __init__(self,
                 d_results: dict,
                 is_debug=False):
        """
        Created:
            29-Mar-2019
            craig.trim@ibm.com
            *   refactored out of 'dimension-computation-orchestrator'
        """
        BaseObject.__init__(self, __name__)
        if not d_results:
            raise MandatoryParamError("Service Result")

        self.is_debug = is_debug
        self.d_results = d_results

    def _type_counter(self,
                      type_name: str) -> Counter:
        """ provide a simple distribution of either
            schema or parent types """
        c = Counter()
        for key in self.d_results:
            for tag in self.d_results[key]["tags"]:
                c.update({tag["type"][type_name]: 1})

        return c

    def _other_counter(self) -> Counter:
        """ provide a distribution on tags parents
            that are classified as 'other' in the schema """
        c = Counter()
        for key in self.d_results:
            for tag in self.d_results[key]["tags"]:
                if tag["type"]["schema"] == "other":
                    c.update({tag["type"]["parent"]: 1})

        return c

    def _entity_counter(self) -> Counter:
        """ provide a distribution of the entities used to derive tags
            use both single entities and entity formations"""
        c = Counter()
        for key in self.d_results:
            for tag in self.d_results[key]["tags"]:
                entities = tag["provenance"]["entities"]
                [c.update({x: 1}) for x in entities]  # entity by entities
                c.update({"-".join(entities): 1})  # single formation

        return c

    def process(self) -> None:

        schema_count = pprint.pformat(self._type_counter("schema"), indent=4)
        parent_count = pprint.pformat(self._type_counter("parent"), indent=4)
        other_count = pprint.pformat(self._other_counter(), indent=4)
        entity_count = pprint.pformat(self._entity_counter(), indent=4)

        if self.is_debug:
            self.logger.debug("\n".join([
                "Service Result Metrics",
                "\tSchema: {}".format(schema_count),
                "\tParent: {}".format(parent_count),
                "\tOther: {}".format(other_count),
                "\tEntities: {}".format(entity_count)]))
