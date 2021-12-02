#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pprint

from base import BaseObject


class DisplacySpanSorter(BaseObject):
    """ The displacy visualizer requires entities sorted
            by the 'start' attribute
            in ASCending order

    Sample Input:
        [    {'start': 196, 'end': 204, 'label': 'SOFT SKILL'},
             {'start': 47, 'end': 56, 'label': 'DATABASE'},
             {'start': 268, 'end': 283, 'label': 'HARD SKILL'},
             {'start': 214, 'end': 223, 'label': 'OTHER'},
             {'start': 475, 'end': 482, 'label': 'SERVICE MANAGEMENT'}
             {'start': 224, 'end': 244, 'label': 'SERVICE MANAGEMENT'},
             {'start': 284, 'end': 295, 'label': 'HARD SKILL'},
             {'start': 112, 'end': 118, 'label': 'SERVICE MANAGEMENT'},
             {'start': 131, 'end': 137, 'label': 'SERVICE MANAGEMENT'},
             {'start': 439, 'end': 453, 'label': 'HARD SKILL'},
             {'start': 456, 'end': 468, 'label': 'SERVICE MANAGEMENT'},
             {'start': 0, 'end': 10, 'label': 'SOFT SKILL'} ]

    Sample Output:
        [    {'start': 0, 'end': 10, 'label': 'SOFT SKILL'},
             {'start': 47, 'end': 56, 'label': 'DATABASE'},
             {'start': 112, 'end': 118, 'label': 'SERVICE MANAGEMENT'},
             {'start': 131, 'end': 137, 'label': 'SERVICE MANAGEMENT'},
             {'start': 196, 'end': 204, 'label': 'SOFT SKILL'},
             {'start': 214, 'end': 223, 'label': 'OTHER'},
             {'start': 224, 'end': 244, 'label': 'SERVICE MANAGEMENT'},
             {'start': 268, 'end': 283, 'label': 'HARD SKILL'},
             {'start': 284, 'end': 295, 'label': 'HARD SKILL'},
             {'start': 439, 'end': 453, 'label': 'HARD SKILL'},
             {'start': 456, 'end': 468, 'label': 'SERVICE MANAGEMENT'},
             {'start': 475, 'end': 482, 'label': 'SERVICE MANAGEMENT'}  ]
    """

    def __init__(self,
                 input_entities: list,
                 is_debug: bool = False):
        """
        Created:
            14-Dec-2019
            craig.trim@ibm.com
            *   refactored out of 'generate-display-spans' in pursuit of
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1594
        :param input_entities:
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)

        self._is_debug = is_debug
        self._input_entities = input_entities

    def process(self) -> list:
        """
        :return:
            a list of entity spans dictionaries sorted by 'start' span position
        """
        d = {}
        for entity in self._input_entities:
            d[entity["start"]] = entity

        entities = []
        for key in sorted(d.keys()):
            entities.append(d[key])

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Displacy Span Sorting Complete",
                f"\tTotal Entities: {len(entities)}",
                pprint.pformat(entities, indent=4)]))

        return entities
