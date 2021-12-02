#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pprint

from base import BaseObject


class MappingTableReversal(BaseObject):
    _d_revmap = {}

    def __init__(self,
                 some_mapping: dict,
                 is_debug: bool = False):
        """
        Given this input:
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

        Create this output:
        {   'access attempt': [
                'UC4_SC5_NETWORK_DRIVE-ACCESS_ISSUE'],
            'access network drive': [
                'UC4_SC5_NETWORK_DRIVE-ACCESS_ISSUE'],
            'hard disk space': [
                'UC4_SC5_NETWORK_DRIVE-LOW_DISK_SPACE'],
            'map drive': [
                'UC4_SC5_NETWORK_DRIVE-MAPPING_ISSUE'],
            'network drive': [
                'UC4_SC5_NETWORK_DRIVE-LOW_DISK_SPACE',
                'UC4_SC5_NETWORK_DRIVE',
                'UC4_SC5_NETWORK_DRIVE-MAPPING_ISSUE',
                'UC4_SC5_NETWORK_DRIVE-ACCESS_ISSUE']}

        Updated:
            17-Jul-2019
            craig.trim@ibm.com
            *   migrated from abacus-att
                git@github.ibm.com:abacus-implementation/abacus-att.git
            *   add debug param, strict typing and more logging
        """
        BaseObject.__init__(self, __name__)

        self.is_debug = is_debug
        self._process(some_mapping)

    def normalize(self) -> None:
        ndict = {}
        for key in self._d_revmap:
            ndict[key] = sorted(list(self._d_revmap[key]))
        self._d_revmap = ndict

    def _process(self,
                 some_mapping: dict) -> None:

        for key in some_mapping:
            for lvalue in some_mapping[key]:
                if not isinstance(some_mapping[key][lvalue], list):
                    continue

                for tag in some_mapping[key][lvalue]:
                    if tag not in self._d_revmap:
                        self._d_revmap[tag] = set()
                    self._d_revmap[tag].add(key)

        self.normalize()
        if self.is_debug:
            self.logger.debug("\n".join([
                "Mapping Table Reversal Completed",
                pprint.pformat(self._d_revmap)]))

    def table(self) -> dict:
        return self._d_revmap
