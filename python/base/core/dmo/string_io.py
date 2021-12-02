#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import datetime
from collections import Counter
from string import ascii_lowercase


class StringIO(object):
    """methods for String I/O"""

    @staticmethod
    def to_date(tts: str) -> str:
        tts = str(tts)
        if "." in tts:
            tts = tts.split(".")[0].strip()

        return datetime.datetime.fromtimestamp(int(tts)).strftime('%Y-%m-%d %H:%M:%S')

    @staticmethod
    def ascii(some_input_lines: list) -> Counter:
        """
        filter a list of text and return a count of ascii-only tokens

        sample input:
            23 qu!ck br0wn f0x jumps over th# lazy d%g ! ! ! ?

        sample output
            [   jumps, over, lazy ]

        :param some_input_lines:
            lines of text of any length and any amount
        :return:
            a unique list of ascii tokens sorted a-z
        """

        def _is_valid(a_token: str) -> bool:
            return sum([(ch in ascii_lowercase) for ch in a_token]) == len(a_token)

        words = Counter()

        for line in some_input_lines:
            tokens = line.split()
            tokens = [x.lower().strip().replace("_", " ") for x in tokens if x]
            tokens = [x for x in tokens if len(x) > 1]
            [words.update({x: 1}) for x in tokens if _is_valid(x)]

        return words
