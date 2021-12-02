#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject


class TextStringMatcher(BaseObject):
    """ Simple Text String matcher """

    def __init__(self,
                 a_token: str,
                 some_text: str,
                 is_debug: bool = False,
                 remove_punctuation: bool = True):
        """
        Created:
            27-Jun-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1142#issuecomment-15377548
        Updated:
            14-Dec-2019
            craig.trim@ibm.com
            *   change matching strategy
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1613#issuecomment-16624767
        Updated:
            14-Jan-2020
            craig.trim@ibm.com
            *   https://github.ibm.com/-cdo/unstructured-analytics/issues/1732#issuecomment-17146815
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._token = a_token.lower().strip()

        def _normalize() -> str:
            if remove_punctuation:  # GIT-1732-17146815
                from nlutext import PunctuationRemover
                return PunctuationRemover(is_debug=self._is_debug,
                                          the_input_text=some_text).process()
            return some_text

        self._input_text = _normalize()

    def process(self) -> bool:
        if self._token == self._input_text:
            return True

        t1 = f"{self._token} "
        if self._input_text.startswith(t1):
            return True

        t2 = f" {self._token}"
        if self._input_text.endswith(t2):
            return True

        t3 = f" {self._token} "
        if t3 in self._input_text:
            return True

        return False
