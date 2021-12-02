#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from base import MandatoryParamError


class RelationshipWeighter(BaseObject):
    """ heuristic assignment of weights to relationships """

    def __init__(self,
                 some_provenance: dict):
        """
        Created:
            28-Mar-2019
            craig.trim@ibm.com
            *   refactored out of 'transform-dim-map'
        :param some_provenance:
            the provenance
            sample input:
            {   'activity': 'system',
                'entities': ['source'] }
        """
        BaseObject.__init__(self, __name__)
        if not some_provenance:
            raise MandatoryParamError("Provenance")

        self.provenance = some_provenance

    def _activities(self):
        """
        two types of data:
            user            user-provided data (e.g. CVs)
            system          system-provided data (e.g. Badges)

        two types of tags:
            supervised      tags have a corresponding entry in the Ontology
            unsupervised    tags are not found in the Ontology

        three types of activities:

            system:         system-data, supervised-tags
                            higher trust, because the system is asserting on behalf
                                of the user

            supervised:     user-data, supervised-tags
                            neutral trust; no reason to disbelieve user
                                but no reason to give a boost

            unsupervised:   user-data, unsupervised-tags
                            lower trust; this is a high-recall tagger
                                and false positives are more likely
        :return:
            a modified weight
        """
        weight = 1.0
        activity = self.provenance["activity"]

        if activity == "system":
            weight *= 1.5
        if activity == "supervised":
            weight *= 1.0
        if activity == "unsupervised":
            weight *= 0.75

        return weight

    def _entities(self):
        weight = 1.0
        entities = self.provenance["entities"]

        if "rel-1" in entities:
            weight -= 0.10
        if "rel-2" in entities:
            weight -= 0.20
        if "rel-3" in entities:
            weight -= 0.40
        if "rel-4" in entities:
            weight -= 0.60
        if "rel-5" in entities:
            weight -= 0.80
        if "rel-6" in entities:
            weight -= 0.95

        if len(entities) > 1:
            weight -= ((len(entities) - 1) / 10)

        if weight < 0:
            weight = 0.01
        if weight > 1:
            weight = 1.0

        return weight

    def process(self) -> float:
        return self._entities() * self._activities()
