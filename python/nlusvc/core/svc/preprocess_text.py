#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import textacy

from base import BaseObject


class PreProcessText(BaseObject):
    """
    """

    def __init__(self,
                 is_debug: bool = False):
        """
        Created:
            28-Jun-2019
            craig.trim@ibm.com
            *   refactored out of text-api
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)
        self.is_debug = is_debug

    @staticmethod
    def remove_text_inside_brackets(input_text: str,
                                    brackets: str = "()[]<>"):
        # https://stackoverflow.com/questions/14596884/remove-text-between-and-in-python
        count = [0] * (len(brackets) // 2)  # count open/close brackets
        saved_chars = []
        for character in input_text:
            for i, b in enumerate(brackets):
                if character == b:  # found bracket
                    kind, is_close = divmod(i, 2)
                    count[kind] += (-1) ** is_close  # `+1`: open, `-1`: close
                    if count[kind] < 0:  # unbalanced bracket
                        count[kind] = 0  # keep it
                    else:  # found bracket to remove
                        break
            else:  # character is not a [balanced] bracket
                if not any(count):  # outside brackets
                    saved_chars.append(character)
        return ''.join(saved_chars)

    def process(self,
                input_text: str,
                lowercase: bool = True,
                no_urls: bool = True,
                no_emails: bool = True,
                no_phone_numbers: bool = True,
                no_numbers: bool = True,
                no_currency_symbols: bool = True,
                no_punct: bool = True,
                no_contractions: bool = True,
                no_accents: bool = True,
                remove_inside_brackets: bool = False) -> str:

        if remove_inside_brackets:
            input_text = self.remove_text_inside_brackets(input_text)

        input_text = textacy.preprocess_text(
            input_text,
            lowercase=lowercase,
            no_urls=no_urls,
            no_emails=no_emails,
            no_phone_numbers=no_phone_numbers,
            no_numbers=no_numbers,
            no_currency_symbols=no_currency_symbols,
            no_punct=no_punct,
            no_contractions=no_contractions,
            no_accents=no_accents)

        return input_text
