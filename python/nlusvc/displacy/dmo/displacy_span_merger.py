#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pprint

from base import BaseObject


class DisplacySpanMerger(BaseObject):
    """
    Purpose:

    """

    def __init__(self,
                 input_entities: list,
                 is_debug: bool = False):
        """
        Created:
            20-Sept-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/972
        Updated:
            11-Oct-2019
            craig.trim@ibm.com
            *   updated logging
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1092
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._input_entities = input_entities

    def process(self) -> list:
        """
        Purpose:
            Perform Entity De-Duplication and Merging

        Notes:
            The use of recursive synonym scanning creates text overlap in span matching

            Consider the given example where
                'are readable by the public' has span (72, 98)
            and
                'readable by the public' has span (76, 98)

            In this case it makes sense to merge these two spans into (72, 98)

        Sample Input:
            [   { 'end': 11,
                  'label': 'BLOCKCHAIN',
                  'start': 4,
                  'text': 'Bitcoin',
                  'type': 'tag'},
                { 'end': 98,
                  'label': 'BLOCKCHAIN',
                  'start': 72,
                  'text': 'are readable by the public',
                  'type': 'inner-syn'},
                { 'end': 98,
                  'label': 'BLOCKCHAIN',
                  'start': 76,
                  'text': 'readable by the public',
                  'type': 'inner-syn'},
                { 'end': 134,
                  'label': 'BLOCKCHAIN',
                  'start': 118,
                  'text': 'cryptocurrencies',
                  'type': 'syn'} ]
        Sample Output:
            [   { 'end': 11,
                  'label': 'BLOCKCHAIN',
                  'start': 4,
                  'text': 'Bitcoin',
                  'type': 'tag'},
                { 'end': 98,
                  'label': 'BLOCKCHAIN',
                  'start': 72,
                  'text': 'are readable by the public',
                  'type': 'inner-syn'},
                { 'end': 134,
                  'label': 'BLOCKCHAIN',
                  'start': 118,
                  'text': 'cryptocurrencies',
                  'type': 'syn'} ]

        :return:
        """
        from nlusvc.displacy.dto import DisplacyEntityGenerator
        update_entities = []
        remove_entities = []

        for e1 in self._input_entities:
            e1x = int(e1["start"])
            e1y = int(e1["end"])

            for e2 in self._input_entities:
                e2x = int(e2["start"])
                e2y = int(e2["end"])

                if e1x == e2x and e1y == e2y:
                    continue

                if e1y == e2y:
                    min_x = min([e1x, e2x])
                    max_y = max([e1y, e2y])

                    e1_span = e1y - e1x
                    e2_span = e2y - e2x

                    def _span_text():
                        if e1_span > e2_span:
                            return e1['text']
                        return e2['text']

                    def _span_type():
                        if e1_span > e2_span:
                            return e1['type']
                        return e2['type']

                    update_entities.append(DisplacyEntityGenerator.generate(text=_span_text(),
                                                                            entity_type=_span_type(),
                                                                            label=e2['label'],
                                                                            start=min_x,
                                                                            end=max_y))
                    remove_entities.append(e1)
                    remove_entities.append(e2)

        def _hash_entities(an_entity_dict) -> float:
            return hash(' '.join([str(y) for y in an_entity_dict.values()]))

        hash_remove = [_hash_entities(x) for x in remove_entities]

        master = update_entities
        for entity in self._input_entities:
            if _hash_entities(entity) not in hash_remove:
                master.append(entity)

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Displacy Span Merger Complete",
                f"\tTotal Entities: {len(master)}",
                pprint.pformat(master, indent=4)]))

        return master
