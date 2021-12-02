#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import os


class LabelFormatter(object):
    """ string utility methods """

    def __init__(self,
                 is_debug: bool = False):
        """
        Updated:
            18-Mar-2019
            craig.trim@ibm.com
            *   renamed from 'string-util'
            *   externalized formats to YAML
        """
        self.is_debug = is_debug
        self.d_format = self._load()

    @staticmethod
    def _load() -> dict:
        from . import FileIO
        path = os.path.join(os.environ["CODE_BASE"],
                            "resources/ontology/filters/entity_formatting.yml")
        return FileIO.file_to_yaml(path)

    def _camel_case(self,
                    a_single_token: str,
                    len_threshold=2):

        # lookup in formatter
        if a_single_token.lower() in self.d_format:
            return self.d_format[a_single_token.lower()]

        return self.camel_case(a_single_token,
                               len_threshold)

    @staticmethod
    def camel_case(a_single_token: str,
                   len_threshold: int = 2,
                   split_tokens: bool = False,
                   enforce_upper_case: bool = True):

        # don't format small tokens
        if len(a_single_token) <= len_threshold:
            if enforce_upper_case:
                return a_single_token.upper()
            if a_single_token.lower() == 'ibm':
                return 'IBM'
            return a_single_token

        if split_tokens and ' ' in a_single_token:
            results = []
            for token in a_single_token.split(' '):
                results.append(LabelFormatter.camel_case(a_single_token=token,
                                                         len_threshold=len_threshold,
                                                         split_tokens=False,
                                                         enforce_upper_case=False))
            return ' '.join(results)

        # perform camel casing
        return "{}{}".format(a_single_token[:1].upper(),
                             a_single_token[1:].lower())

    def process(self,
                a_token: str) -> str:

        a_token = a_token.replace("_", " ")
        tokens = [self._camel_case(x) for x in a_token.split(" ")]
        tokens = [x for x in tokens if x]

        return " ".join(tokens)
