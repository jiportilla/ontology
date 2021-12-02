#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import os
import pprint

from base import BaseObject
from base import FileIO


class GeneratePatterns(BaseObject):
    """ Generate the NLU Patterns for each token into a file
        that is used by the NLU Parser at runtime """

    def __init__(self,
                 ontology_name: str,
                 is_debug: bool = False):
        """
        Created:
            26-Mar-2019
            craig.trim@ibm.com
        Updated:
            13-Dec-2019
            craig.trim@ibm.com
            *   add ontology-name as a param
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1583
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._ontology_name = ontology_name

    def process(self) -> dict:
        from taskmda.mda.dmo import EntityPatternGenerator
        from taskmda.mda.dto import KbNames
        from taskmda.mda.dto import KbPaths
        from taskmda.mda.dmo import GenericTemplateAccess

        svcresult = EntityPatternGenerator(is_debug=self._is_debug,
                                           ontology_name=self._ontology_name).process()

        the_json_result = pprint.pformat(svcresult["patterns"], indent=4)
        the_json_result = "{0} = {{\n {1}".format(
            KbNames.patterns(), the_json_result[1:])

        the_template_result = GenericTemplateAccess.process()
        the_template_result = the_template_result.replace(
            "CONTENT", the_json_result)

        path = os.path.join(os.environ["CODE_BASE"],
                            KbPaths.patterns(ontology_name=self._ontology_name))
        FileIO.text_to_file(path, the_template_result)

        return svcresult
