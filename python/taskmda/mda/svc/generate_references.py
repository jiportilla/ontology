#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import os
import pprint

from base import BaseObject
from base import FileIO


class GenerateReferences(BaseObject):
    """
    Prereqs:
        `source admin.sh wiki lookup`
        `source admin.sh wiki tag`
    """

    def __init__(self,
                 is_debug: bool = False):
        """
        Created:
            7-May-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/143
        """
        BaseObject.__init__(self, __name__)
        self.input = self._input_file()
        self._is_debug = is_debug

    @staticmethod
    def _input_file() -> dict:
        relative_path = "resources/output/wikipedia/dbpedia_02_summary_tagging.json"
        return FileIO.file_to_json(relative_path)

    def process(self):
        from taskmda.mda.dto import KbNames
        from taskmda.mda.dto import KbPaths
        from taskmda.mda.dmo import GenericTemplateAccess
        from taskmda.mda.dmo import WikipediaReferenceGenerator

        references = WikipediaReferenceGenerator().process()
        the_json_result = pprint.pformat(references, indent=4)
        the_json_result = "{0} = {{\n {1}".format(
            KbNames.references(), the_json_result[1:])

        the_template_result = GenericTemplateAccess.process()
        the_template_result = the_template_result.replace(
            "CONTENT", the_json_result)

        path = os.path.join(os.environ["CODE_BASE"],
                            KbPaths.references())
        FileIO.text_to_file(path, the_template_result)
