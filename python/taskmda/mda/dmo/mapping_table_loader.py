#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pprint

from base import BaseObject


class MappingTableLoader(BaseObject):
    """
    Purpose:
        normalizes the mapping table into a computationally efficient and non ambiguous structure

        Example-1:
            Given this input:
            {   "UC4_SC5_NETWORK_DRIVE": {
                    "include_all_of": ["network drive"],
                    "UC4_SC5_NETWORK_DRIVE-MAPPING_ISSUE": {
                        "include_all_of": ["map drive"]},
                        "UC4_SC5_NETWORK_DRIVE-LOW_DISK_SPACE": {
                            "include_all_of": ["hard disk space"]},
                        "UC4_SC5_NETWORK_DRIVE-ACCESS_ISSUE": {
                            "include_one_of": ["access network drive", "access attempt"]}
                    }}}

            Create this output:
            {   'UC4_SC5_NETWORK_DRIVE': {
                    'exclude_all_of': [],
                    'exclude_one_of': [],
                    'include_all_of': ['network drive'],
                    'include_one_of': []},
                'UC4_SC5_NETWORK_DRIVE-ACCESS_ISSUE': {
                    'exclude_all_of': [],
                    'exclude_one_of': [],
                    'include_all_of': ['network drive'],
                    'include_one_of': ['access network drive', 'access attempt']},
                'UC4_SC5_NETWORK_DRIVE-LOW_DISK_SPACE': {
                    'exclude_all_of': [],
                    'exclude_one_of': [],
                    'include_all_of': ['hard disk space',
                    'network drive'],
                    'include_one_of': []},
                'UC4_SC5_NETWORK_DRIVE-MAPPING_ISSUE': {
                    'exclude_all_of': [],
                    'exclude_one_of': [],
                    'include_all_of': ['map drive', 'network drive'],
                    'include_one_of': []}}

            Note how all tags are inherited from parent to child

        Example-2:

            Given this input:
            'CONTRACT_VALIDATE_NUMBER_NOT_RED_2': {
                'exclude_one_of':   [ 'completed'],
                'include_all_of':   [ 'not red', 'single portal'],
                'include_one_of':   [ 'contract', 'contract number', 'find contract '

            Create this output:
            'CONTRACT_VALIDATE_NUMBER_NOT_RED_2': {
                'deduction':        False,
                'exclude_all_of':   [],
                'exclude_one_of':   [ 'completed'],
                'exclusive':        False,
                'include_all_of':   [ 'not red', 'single portal'],
                'include_one_of':   [ 'contract', 'contract number', 'find contract ' ]

            Note how default tags (deduction, exclusive) are represented
            and empty lists are present.

    """

    _d_mapping = {}

    def __init__(self,
                 some_mapping: dict,
                 is_debug: bool = False):
        """
        Updated:
            21-Jun-2017
            craig.trim@ibm.com
            *   inject the dictionary in support of
                https://github.ibm.com/abacus-implementation/Abacus/issues/1624
        Updated:
            17-Jul-2019
            craig.trim@ibm.com
            *   migrated from abacus-att
                git@github.ibm.com:abacus-implementation/abacus-att.git
        :param some_mapping:
            the mapping table to use
        """
        BaseObject.__init__(self, __name__)

        self.is_debug = is_debug
        self._process(some_mapping)

    @staticmethod
    def get_value(some_dict: dict,
                  some_key: str) -> list:
        if some_key in some_dict:
            return some_dict[some_key]
        return []

    @staticmethod
    def get_bool_value(some_dict: dict,
                       some_key: str) -> bool:
        if some_key in some_dict:
            return some_dict[some_key]
        return False

    @staticmethod
    def merge_parent_values(some_entry: dict,
                            some_parent: dict) -> None:
        """
        helper method
        merge current entries with parent entries
        :param some_entry:
        :param some_parent:
        """
        some_entry["include_all_of"] = some_entry["include_all_of"] + some_parent["include_all_of"]
        some_entry["include_one_of"] = some_entry["include_one_of"] + some_parent["include_one_of"]
        some_entry["exclude_all_of"] = some_entry["exclude_all_of"] + some_parent["exclude_all_of"]
        some_entry["exclude_one_of"] = some_entry["exclude_one_of"] + some_parent["exclude_one_of"]

    def get_values(self,
                   some_entry: dict) -> dict:
        """
        :param some_entry:
        :return: initial dictionary entry for this key
        """
        return {
            "include_all_of": self.get_value(some_entry, "include_all_of"),
            "include_one_of": self.get_value(some_entry, "include_one_of"),
            "exclude_all_of": self.get_value(some_entry, "exclude_all_of"),
            "exclude_one_of": self.get_value(some_entry, "exclude_one_of"),
            "exclusive": self.get_bool_value(some_entry, "exclusive"),
            "deduction": self.get_bool_value(some_entry, "deduction")}

    def load(self,
             some_dict: dict,
             some_parent: str or None) -> None:
        """
        recurse through entire table and add inherited tags
        :param some_dict:
        :param some_parent:
        """
        for key in some_dict:

            if not isinstance(some_dict[key], dict):
                continue

            self._d_mapping[key] = self.get_values(some_dict[key])

            if some_parent is not None:
                self.merge_parent_values(self._d_mapping[key], some_parent)

            self.load(some_dict[key], self._d_mapping[key])

    def _process(self,
                 some_mapping: dict) -> None:
        self.load(some_mapping, None)
        if self.is_debug:
            self.logger.debug("\n".join([
                "Mapping Table Loaded",
                pprint.pformat(self._d_mapping)]))

    def table(self) -> dict:
        return self._d_mapping
