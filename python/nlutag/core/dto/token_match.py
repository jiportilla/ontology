#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pprint
from typing import Optional

from base import BaseObject


class TokenMatches(BaseObject):
    """ Token Matches Structure """

    def __init__(self, some_matches=None):
        """
        Updated:
            10-Jan-2020
            craig.trim@ibm.com
            *   add x,y coords to 'add' function
        :param some_matches:
        """
        BaseObject.__init__(self, __name__)
        if some_matches is None:
            some_matches = {}

        self.matches = some_matches

    def get_matches(self):
        return self.matches

    @staticmethod
    def normalize_key(some_key):
        return some_key.replace("_", " ").lower().strip()

    def get_keys(self):
        return self.matches.keys()

    def get_len(self):
        return len(self.get_keys())

    def get_by_key(self, some_key):
        the_normalized_key = self.normalize_key(some_key)
        if the_normalized_key in self.matches:
            return self.matches[the_normalized_key]
        return None

    def log(self):
        if self.get_len() > 0:
            self.logger.debug('\n'.join([
                "Token Matches",
                pprint.pformat(self.get_matches(), indent=4)]))
        else:
            self.logger.debug("No Token Matches")

    @staticmethod
    def normalize_tuple(some_tuple):
        """
        Purpose:
            Tuples need to be transformed to either strings or lists
            as python tuples are not easily preserved in JSON
        Example:
            given the input:
                (cap, desktop)
            give the output:
                "cap+desktop"
        Created:
            28-Sept-2016
            craig.trim@ibm.com
        :param some_tuple:
        :return:
            string version of tuple
        """
        if isinstance(some_tuple, tuple):
            if 2 == len(some_tuple):
                return "{0}+{1}".format(
                    some_tuple[0],
                    some_tuple[1])
            elif 3 == len(some_tuple):
                return "{0}+{1}+{2}".format(
                    some_tuple[0],
                    some_tuple[1],
                    some_tuple[2])
            elif 4 == len(some_tuple):
                return "{0}+{1}+{2}{3}".format(
                    some_tuple[0],
                    some_tuple[1],
                    some_tuple[2],
                    some_tuple[3])
        return some_tuple

    def exists(self, some_key):
        return self.get_by_key(some_key) is not None

    def add(self,
            some_key: str,
            some_match: str,
            some_type: str,
            some_sub_type: str,
            some_confidence: float):
        """
        Purpose:
            defines a standard way to implement a "token-match" structure
        :param some_key:
        :param some_match:
        :param some_type:
        :param some_sub_type:
        :param some_confidence:
        :return:
        """
        the_normalized_key = self.normalize_key(some_key)
        if the_normalized_key not in self.matches:
            self.matches[the_normalized_key] = {}
            self.matches[the_normalized_key]["matches"] = []

        self.matches[the_normalized_key]["matches"].append({
            "token": self.normalize_tuple(some_match),
            "confidence": some_confidence,
            "provenance": {
                "type": some_type,
                "subType": some_sub_type}})

        return self.matches[the_normalized_key]
