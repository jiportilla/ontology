#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pprint
from typing import Optional

from base import BaseObject
from nlusvc.core.svc import TextStringMatcher


class CoordMatcherText(BaseObject):
    """ Provide (X,Y) Coordinates for Location of Entity Text in an Input String """

    def __init__(self,
                 input_text: str,
                 entity_text: str,
                 is_debug: bool = False):
        """
        Created:
            10-Jan-2020
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1722
        Updated:
            14-Jan-2020
            craig.trim@ibm.com
            *   https://github.ibm.com/-cdo/unstructured-analytics/issues/1732#issuecomment-17146815
        Updated:
            15-Jan-2020
            craig.trim@ibm.com
            *   renamed from 'exact-coord-matcher'
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._input_text = input_text
        self._entity_text = entity_text

    def process(self) -> Optional[dict]:
        from nlusvc.coords.dto import MatchStructure
        normalized_input_text = self._input_text.lower().strip()

        entity_text = self._entity_text.lower().strip()
        if entity_text not in normalized_input_text:
            entity_text = entity_text.replace(' ', '_')
        if entity_text not in normalized_input_text:
            entity_text = entity_text.replace('_', ' ')

        has_match = TextStringMatcher(is_debug=self._is_debug,
                                      a_token=entity_text,
                                      some_text=normalized_input_text,
                                      remove_punctuation=False).process()  # GIT-1732-17146815

        # if entity_text in normalized_input_text:
        if has_match:
            x = normalized_input_text.index(entity_text)
            y = x + len(entity_text)

            if x >= 0 and y >= 0 and y - x > 0:

                match_text = self._input_text[x:y]

                svcresult = MatchStructure.generate(
                    input_string=normalized_input_text,
                    entity_text=entity_text,
                    match_text=match_text,
                    x=x, y=y)

                if self._is_debug:
                    self.logger.debug('\n'.join([
                        "Exact Coordinate Match Found",
                        pprint.pformat(svcresult, indent=4)]))

                return svcresult

        if self._is_debug:
            self.logger.debug('\n'.join([
                "No Exact Match",
                f"\tEntity: {entity_text}",
                f"\tInput Text: {normalized_input_text}"]))

        return None
