#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import os
import pprint

from base import FileIO


class GenerateSiblingsDict:
    """
    Updated:
        21-Feb-2019
        craig.trim@ibm.com
        *   migrated to text
    """

    @classmethod
    def process(cls):
        from taskmda.mda.dto import KbNames
        from taskmda.mda.dto import KbPaths
        from taskmda.mda.dto import KbResources
        from taskmda.mda.dmo import SiblingsKbReader
        from taskmda.mda.dmo import SiblingsDictGenerator
        from taskmda.mda.dmo import GenericTemplateAccess

        data_frame = SiblingsKbReader(KbResources().siblings()).read_csv()

        generator = SiblingsDictGenerator(data_frame)
        the_result = generator.process()

        the_json_result = pprint.pformat(the_result, indent=4)
        the_json_result = "{0} = {{\n {1}".format(
            KbNames.siblings(), the_json_result[1:])

        the_template_result = GenericTemplateAccess.process()
        the_template_result = the_template_result.replace(
            "CONTENT", the_json_result)

        path = os.path.join(os.environ["CODE_BASE"],
                            KbPaths.siblings())
        FileIO.text_to_file(path, the_template_result)

        return the_result
