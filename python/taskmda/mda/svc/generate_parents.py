#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import os
import pprint

from base import BaseObject
from base import FileIO


class GenerateParents(BaseObject):
    """ Generate the parent (type) for each term
        this is the SINGLE SOURCE OF TRUTH for parents in the entire system

        this generated file can also be used to find ancestry via transitive lookups
    """

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

    def process(self):
        from taskmda.mda.dmo import EntityParentGenerator
        from taskmda.mda.dto import KbNames
        from taskmda.mda.dto import KbPaths
        from taskmda.mda.dmo import GenericTemplateAccess

        labels = EntityParentGenerator(ontology_name=self._ontology_name).process()

        the_json_result = pprint.pformat(labels, indent=4)
        the_json_result = "{0} = {{\n {1}".format(
            KbNames.parents(), the_json_result[1:])

        the_template_result = GenericTemplateAccess.process()
        the_template_result = the_template_result.replace(
            "CONTENT", the_json_result)

        relative_path = KbPaths.parents(ontology_name=self._ontology_name)
        absolute_path = os.path.join(os.environ["CODE_BASE"], relative_path)
        FileIO.text_to_file(absolute_path, the_template_result)
