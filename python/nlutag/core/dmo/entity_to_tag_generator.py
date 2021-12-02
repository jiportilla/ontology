#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import os
import pprint

from base import BaseObject
from nlutag.core.dto.token_match import TokenMatches


class EntityToTagGenerator(BaseObject):
    def __init__(self,
                 is_debug: bool = False):
        """
        Created:
            9-Feb-2017
            craig.trim@ibm.com
            *   generate simple string-based tags from more complex entity matches
        Updated:
            21-Feb-2019
            craig.trim@ibm.com
            *   migrated from -text
            *   rewritten from the ground up (simplified)
        Updated:
            25-Apr-2019
            craig.trim@ibm.com
            *   changed instantiation strategy
        Updated:
            21-Aug-2019
            craig.trim@ibm.com
            *   put tag-confidence-threshold into environment
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/796
        """
        BaseObject.__init__(self, __name__)
        self.is_debug = is_debug
        self._threshold = float(os.environ['TAG_CONFIDENCE_THRESHOLD'])

        if self.is_debug:
            self.logger.debug("Instantiate EntityToTagGenerator¬")

    @staticmethod
    def _list_by_confidence(token_matches: TokenMatches) -> list:
        """
        Purpose:
            filter out entities with low confidence
        Sample Input:
            'steady state support'

            may have been located via two match patterns
                'steadi+state+supporting', confidence=20%
                'steady state support', confidence=90%
        Implementation:
            -   the first match pattern is a long-distance formation with low confidence
                the low match will be discarded
            -   the second match pattern is an exact match with high confidence
                the high match will be kept
        Sample Output:
            'steady state support' will be considered a valid entity
        :return:
            a list of tuple values (tag, confidence)
            [   ('pmi', 100),
                ('plan', 100),
                ('rational test', 94),
                ...
                ('rational tester', 94),
                ('test plan', 7) ]
        """
        valid_labels = []

        for label in token_matches.get_keys():
            entity_structure = token_matches.get_by_key(label)
            max_confidence = max([x["confidence"]
                                  for x in entity_structure["matches"]])
            valid_labels.append((label, max_confidence))

        return valid_labels

    def process(self,
                entities: TokenMatches) -> list:
        result = self._list_by_confidence(entities)

        if self.is_debug:
            self.logger.debug("\n".join([
                "Entity to Tag Generation Results",
                "\t{}".format(pprint.pformat(result, indent=4))]))

        return result
