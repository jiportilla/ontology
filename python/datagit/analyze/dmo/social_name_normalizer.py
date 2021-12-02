# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from nlutext import PunctuationRemover


class SocialNameNormalizer(BaseObject):
    """ Normalizes a Name from Social Text
        Sample Input:
            [john-doe1]
        Sample Output:
            [john-doe]
    """

    def __init__(self,
                 input_text: str,
                 is_debug: bool = True):
        """
        Created:
            31-Dec-2019
            craig.trim@ibm.com
            *   refactored out of 'comment-node-builder'
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1681#issuecomment-16873584
        :param is_debug:
            if True     increase log output at DEBUG level
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._input_text = input_text

    def process(self) -> str:
        token = PunctuationRemover(is_debug=False,
                                   the_input_text=self._input_text).process().strip()
        if ' ' in token:  # take first token only
            token = token.split(' ')[0].strip()
        if token[-1:].isdigit():  # remove trailing digits
            token = token[:len(token) - 1]
        return token.lower().strip()
