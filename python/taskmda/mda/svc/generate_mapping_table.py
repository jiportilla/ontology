#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import os
import pprint

from base import BaseObject
from base import FileIO


class GenerateMappingTable(BaseObject):

    def __init__(self,
                 is_debug:bool=False):
        """
        Created:
            17-Jul-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/453
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self.input = self._input_file()

    @staticmethod
    def _input_file() -> dict:
        _d_master = {}
        relative_path = "resources/dialog/mapping/"
        for file in FileIO.load_files(relative_path, "yml"):
            d_file = FileIO.file_to_yaml_by_relative_path(file)
            _d_master = {**d_file, **_d_master}

        return _d_master

    @staticmethod
    def _normalize(_d_input: dict) -> dict:
        """
        Purpose:
        Transform the YAML Flow Format into a specialized JSON Dictionary Format

        Rationale:
        The Abacus AT&T code leverages the prescribed JSON dictionary format below for
        service catalog matching of tags to flows.  Rather than adapt the existing logic
        to the new YAML flow format, it was felt easier to transform the YAML flow format
        to be compatible with the expected dictionary format.

        :param _d_input:
            any YAML flow doc

            Sample Input (YAML Flow Format):
                CONTRACT_VALIDATE_NUMBER_NOT_RED:
                  -
                    exclude_one_of:
                      - completed
                    include_all_of:
                      - not red
                      - contract number
                  -
                    exclude_one_of:
                      - completed
                    include_all_of:
                      - not red
                      - single portal
                    include_one_of:
                      - contract
                      - contract number
                      - find contract numer
        :return:
            a JSON dictionary

            Sample Output (JSON Dictionary Format):
                'CONTRACT_VALIDATE_NUMBER_NOT_RED_1': {
                    'exclude_one_of': [ 'completed'],
                    'include_all_of': [ 'not red',
                                        'contract number'] },
                'CONTRACT_VALIDATE_NUMBER_NOT_RED_2': {
                    'exclude_one_of': [ 'completed'],
                    'include_all_of': [ 'not red',
                                        'single portal'],
                    'include_one_of': [ 'contract',
                                        'contract number',
                                        'find contract '
                                        'numer'] }
        """

        d_normalized = {}
        for flow_name in _d_input:

            ctr = 1
            for mapping in _d_input[flow_name]:
                mapping_name = "{}_{}".format(flow_name, ctr)
                d_normalized[mapping_name] = mapping
                ctr += 1

        return d_normalized

    @staticmethod
    def _to_file(d_result: dict,
                 kb_name: str,
                 kb_path: str):
        from taskmda.mda.dmo import GenericTemplateAccess

        the_json_result = pprint.pformat(d_result, indent=4)
        the_json_result = "{0} = {{\n {1}".format(
            kb_name, the_json_result[1:])

        the_template_result = GenericTemplateAccess.process()
        the_template_result = the_template_result.replace(
            "CONTENT", the_json_result)

        path = os.path.join(os.environ["CODE_BASE"],
                            kb_path)
        FileIO.text_to_file(path, the_template_result)

    def process(self):
        from taskmda.mda.dto import KbNames
        from taskmda.mda.dto import KbPaths
        from taskmda.mda.dmo import MappingTableLoader
        from taskmda.mda.dmo import MappingTableReversal

        _d_mapping = self._normalize(self._input_file())

        _d_fwd_map = MappingTableLoader(some_mapping=_d_mapping).table()
        self._to_file(_d_fwd_map,
                      KbNames.mapping(),
                      KbPaths.mapping())

        _d_rev_map = MappingTableReversal(some_mapping=_d_mapping).table()
        self._to_file(_d_rev_map,
                      KbNames.mapping_rev(),
                      KbPaths.mapping_rev())
