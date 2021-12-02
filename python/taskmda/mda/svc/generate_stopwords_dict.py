#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import os
import pprint

from base import BaseObject
from base import FileIO


class GenerateStopwordsDict(BaseObject):
    """ Generate the StopWords Dictionary """

    def __init__(self,
                 is_debug: bool = False):
        """
        Updated:
            21-Feb-2019
            craig.trim@ibm.com
            *   migrated to text
        Updated:
            13-Dec-2019
            craig.trim@ibm.com
            *   remove csv-line-reader in favor of file-io
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug

    @staticmethod
    def get_lines() -> list:
        from taskmda.mda.dto import KbResources

        return FileIO.file_to_lines(KbResources().stop_words())

    def process(self):
        from taskmda.mda.dto import KbNames
        from taskmda.mda.dto import KbPaths
        from taskmda.mda.dmo import GenericTemplateAccess

        the_result = self.get_lines()

        the_json_result = pprint.pformat(the_result, indent=4)
        the_json_result = "{0} = [\n {1}".format(KbNames.stopwords(),
                                                 the_json_result[1:])

        the_template_result = GenericTemplateAccess.process()
        the_template_result = the_template_result.replace(
            "CONTENT", the_json_result)

        path = os.path.join(os.environ["CODE_BASE"],
                            KbPaths.stopwords())
        FileIO.text_to_file(path, the_template_result)
