#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import os
import pprint

from base import BaseObject
from base import FileIO


class GenerateFlowTaxonomy(BaseObject):

    def __init__(self,
                 is_debug: bool = False):
        """
        Created:
            17-Jul-2019
            craig.trim@ibm.com
            *   based on 'taxonomy-dict-generator' from the abacus-att project
                git@github.ibm.com:abacus-implementation/abacus-att.git
            *   reference
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/453
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug

    @staticmethod
    def _lines():
        """
        TODO:   in the abacus-att project there was a file called
                    flow_taxonomy_kb.csv
                this explicit taxonomy was loaded and transformed into JSON
                *** WE DON'T NEED TO MAINTAIN AN EXPLICIT TAXONOMY ***
                we can dynamically re-create what that file looked like based on
                analyzing the current flow mapping definitions
        :return:
        """
        return ['CONTRACT_VALIDATE_NUMBER_NOT_RED', 'CHITCHAT_GREETING']

    @staticmethod
    def get_revmap(some_dict):
        revmap = {}

        for key in some_dict:
            for value in some_dict[key]:
                revmap[value] = key

        return revmap

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
        from taskmda.mda.dmo import TaxonomyDictGenerator

        the_taxonomy_dict = TaxonomyDictGenerator(self._lines()).process()
        the_taxonomy_revmap = self.get_revmap(the_taxonomy_dict)

        self._to_file(the_taxonomy_dict,
                      KbNames.flow_taxonomy(),
                      KbPaths.taxonomy())

        self._to_file(the_taxonomy_revmap,
                      KbNames.flow_taxonomy_revmap(),
                      KbPaths.taxonomy_revmap())
