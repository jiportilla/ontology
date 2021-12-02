#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pprint

from base import BaseObject
from base import MandatoryParamError


class ExtractExtendedRelationships(BaseObject):
    """
    Return extended relationships for each input

        sample input:
            [   {"name": "Application Developer"},
                ... ]

        sample output:
            {   'iterations': {
                    'rel-1': {
                        'relationships':  {
                            'parents': [ 'developer' ]}},
                    'rel-2': {
                        'relationships': {
                            'implies': [ 'programming skill'],
                            'parents': [ 'technical role']}},
                    'rel-3': {
                        'relationships': {
                            'implies': [   'software',
                                           'technical skill',
                                           'technical services'],
                            'parents': [   'technical skill',
                                           'individual role']},
                    'rel-4': {
                        'relationships': {
                            'parents': [   'role',
                                           'product',
                                           'industry',
                                           'skill']}}}

        as the relationships continue to branch away from the original input tag
        the inference power decreases
    """

    def __init__(self,
                 some_tags: list,
                 ontology_name: str = 'base',
                 number_of_levels: int = 6,
                 cleanse: bool = True,
                 is_debug: bool = False):
        """
        Created:
            5-Apr-2019
            craig.trim@ibm.com
            *   refactored out of 'generate-dimensions'
        Updated:
            13-Dec-2019
            craig.trim@ibm.com
            *   load dictionaries by ontology name
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1582
        :param some_tags:
            a list of tags
            sample input:
                [   {"name": "Application Developer"},
                    ...
                ]
        """
        BaseObject.__init__(self, __name__)
        from cendantdim.batch.dmo import RelationshipExtractor

        if not some_tags:
            raise MandatoryParamError("Tags")

        self.cleanse = cleanse
        self.d_tags = some_tags
        self.is_debug = is_debug

        self.number_of_levels = number_of_levels
        self.extractor = RelationshipExtractor(is_debug=is_debug,
                                               ontology_name=ontology_name)

    @staticmethod
    def _rel_total(d_rels: dict) -> int:
        total = 0
        for k in d_rels["relationships"]:
            total += len(d_rels["relationships"][k])

        return total

    @staticmethod
    def _cleanse(svcresult: dict) -> dict:
        """
        :param svcresult:
            original output
        :return:
            a concise version of the output
            with debug-level or empty portions removed
        """
        _normalized = {
            "tags": svcresult["tags"],
            "iterations": {}}

        for iteration in svcresult["iterations"]:

            _iteration = {}
            d_iteration = svcresult["iterations"][iteration]

            for relationship in d_iteration["relationships"]:
                values = d_iteration["relationships"][relationship]
                if len(values):
                    _iteration[relationship] = sorted(values)

            if len(_iteration):
                _normalized["iterations"][iteration] = _iteration

        return _normalized

    def process(self) -> dict:

        _cleansed_tags = sorted(set([x["name"].lower().strip() for x in self.d_tags]))
        svcresult = {
            "tags": _cleansed_tags,
            "iterations": {}
        }

        def _iterate() -> dict:
            d_rels_1 = self.extractor.initialize(self.d_tags)
            svcresult["iterations"]["rel-1"] = d_rels_1
            if self._rel_total(d_rels_1) == 0 or self.number_of_levels == 1:
                return svcresult

            d_rels_2 = self.extractor.update(d_rels_1)
            svcresult["iterations"]["rel-2"] = d_rels_2
            if self._rel_total(d_rels_2) == 0 or self.number_of_levels == 2:
                return svcresult

            d_rels_3 = self.extractor.update(d_rels_2)
            svcresult["iterations"]["rel-3"] = d_rels_3
            if self._rel_total(d_rels_3) == 0 or self.number_of_levels == 3:
                return svcresult

            d_rels_4 = self.extractor.update(d_rels_3)
            svcresult["iterations"]["rel-4"] = d_rels_4
            if self._rel_total(d_rels_4) == 0 or self.number_of_levels == 4:
                return svcresult

            d_rels_5 = self.extractor.update(d_rels_4)
            svcresult["iterations"]["rel-5"] = d_rels_5
            if self._rel_total(d_rels_5) == 0 or self.number_of_levels == 5:
                return svcresult

            d_rels_6 = self.extractor.update(d_rels_5)
            svcresult["iterations"]["rel-6"] = d_rels_6
            return svcresult

        svcresult = _iterate()
        if self.cleanse:
            svcresult = self._cleanse(svcresult)

        if self.is_debug:
            self.logger.debug("\n".join([
                "Generated Extended Relationships",
                "\tinput: {}".format(self.d_tags),
                pprint.pformat(svcresult, indent=4)
            ]))

        return svcresult
