#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject


class DisplacySpanConfidence(BaseObject):
    """ Compute a Confidence Level for Displacy Spans

    Sample Input:
        [   {   'label': 'agent',
                'start': 15,
                'end': 23,
                'text': 'employee',
                'type': 'tag'},

            {   'label': 'skill',
                'start': 30,
                'end': 33,
                'text': 'artificial intelligence',
                'type': 'tag'},

            {   'label': 'other',
                'start': 33,
                'end': 94,
                'text': 'team lead',
                'type': 'tag'},

            {   'label': 'skill',
                'start': 47,
                'end': 61,
                'text': 'presentation',
                'type': 'tag'}]

    Sample Output:
        [   {   'label': 'agent',
                'start': 15,
                'end': 23,
                'text': 'employee',
                'type': 'tag',
                'span_confidence': 1.00},

            {   'label': 'skill',
                'start': 30,
                'end': 33,
                'text': 'artificial intelligence',
                'type': 'tag',
                'span_confidence': 1.00},

            {   'label': 'other',
                'start': 33,
                'end': 94,
                'text': 'team lead',
                'type': 'tag',
                'span_confidence': 0.15},

            {   'label': 'skill',
                'start': 47,
                'end': 61,
                'text': 'presentation',
                'type': 'tag',
                'span_confidence': 0.86}]

    """

    def __init__(self,
                 input_entities: list,
                 is_debug: bool = False):
        """
        Created:
            3-Feb-2020
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1803#issuecomment-17531208
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._input_entities = input_entities

    def process(self) -> list:
        for entity in self._input_entities:

            text_size = float(len(entity['text']))
            span_size = float(entity['end'] - entity['start'])

            result = round(text_size / span_size, 2)
            if result > 1:
                result = 1
            elif result < 0:
                result = 0

            entity['span_confidence'] = result

        return self._input_entities
