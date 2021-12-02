#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pprint

from base import BaseObject


class CharacterLevelTokenizer(BaseObject):
    """  Perform Character Level Tokenization """

    def __init__(self,
                 input_text: str,
                 is_debug: bool = False):
        """
        Created:
            14-Jan-2020
            craig.trim@ibm.com
            *   the GitHub documentation is extremely detailed:
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1732
        :param input_text:
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._input_text = input_text

    def _mask_input(self) -> list:
        def _is_punctuation(some_char: str) -> bool:
            if some_char == '-':
                return True
            if some_char.isdigit():
                return False
            if some_char.isnumeric():
                return False
            if some_char.isalpha():
                return False
            return True

        return [_is_punctuation(x) for x in self._input_text]

    @staticmethod
    def _apply_mask(mask: list) -> list:
        buffer = []
        master = []

        flag_F = False
        flag_T = False

        for i in range(0, len(mask)):

            if not mask[i]:

                if flag_F and flag_T:
                    master.append(buffer)
                    buffer = []
                    flag_T = False

                flag_F = True
                buffer.append(mask[i])

            else:
                flag_T = True
                buffer.append(mask[i])

        if len(buffer) > 0:
            master.append(buffer)

        return master

    def _tokenize(self,
                  master: list) -> dict:

        ctr = 0
        d_tokens = {}

        original_buffer = []
        token_buffer = []

        for i in range(0, len(master)):
            buffer = master[i]

            for j in range(0, len(buffer)):
                mask = buffer[j]

                if not mask:
                    token_buffer.append(self._input_text[ctr])
                original_buffer.append(self._input_text[ctr])
                ctr += 1

            d_tokens[i] = {
                "original": ''.join(original_buffer),
                "normalized": ''.join(token_buffer)}

            original_buffer = []
            token_buffer = []

        return d_tokens

    def process(self) -> dict:

        # Step 1: Mask the Input
        mask = self._mask_input()  # GIT-1732-17144827

        # Step 2: Apply the Mask
        master = self._apply_mask(mask)  # GIT-1732-17144919

        # Step 3: Tokenize the Input
        d_tokens = self._tokenize(master)  # GIT-1732-17145376

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Character Level Tokenization Complete",
                f"\tInput Text: {self._input_text}",
                pprint.pformat(d_tokens)]))

        return d_tokens
