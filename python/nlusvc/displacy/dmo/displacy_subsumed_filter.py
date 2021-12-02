#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pprint

from base import BaseObject


class DisplacySubsumedFilter(BaseObject):
    """ Remove Subsumed Tags

    Sample Input:
        [   {   'label': 'company',
                'ontology': 'en_core_web_sm',
                'span_confidence': 1.0,
                'start': 87,
                'end': 101,
                'text': 'GNS Healthcare',
                'type': 'ner'},
            {   'label': 'other',
                'ontology': 'base',
                'span_confidence': 1.0s,
                'start': 91,
                'end': 103,
                'text': 'health informatics',
                'type': 'tag'}]

    Analysis:
        1.  'GNS Healthcare' is (87, 101) and len=14
        2.  'health informatics' is (91, 103) and len=12

    Sample Output:
        [   {   'label': 'company',
                'ontology': 'en_core_web_sm',
                'span_confidence': 1.0,
                'start': 87,
                'end': 101,
                'text': 'GNS Healthcare',
                'type': 'ner'}]

     """

    def __init__(self,
                 input_entities: list,
                 is_debug: bool = False):
        """
        Created:
            3-Feb-2020
            craig.trim@ibm.com
            *   https://github.ibm.com/-cdo/unstructured-analytics/issues/1803#issuecomment-17531165
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._input_entities = input_entities

    def process(self) -> list:
        subsumed = []

        for e1 in self._input_entities:
            for e2 in self._input_entities:
                if e1 == e2:
                    continue

                c_x = e1['start'] > e2['start']
                c_y = e1['start'] < e2['end']

                if c_x and c_y:
                    subsumed.append(e2)

        entities = [entity for entity in self._input_entities
                    if entity not in subsumed]

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Subsumption Analysis Complete",
                f"\tSubsumed Entities (total={len(subsumed)})",
                pprint.pformat(subsumed, indent=4)]))

        return entities
