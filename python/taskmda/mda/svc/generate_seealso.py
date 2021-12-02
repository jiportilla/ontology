#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import os
import pprint

from base import BaseObject
from base import FileIO
from datagraph import OwlGraphConnector


class GenerateSeeAlso(BaseObject):

    def __init__(self,
                 ontology_name: str,
                 is_debug: bool = False):
        """
        Created:
            27-Sept-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1020
        Updated:
            21-Nov-2019
            craig.trim@ibm.com
            *   altered in pursuit of value list change
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1195#issuecomment-16167509
        Updated:
            25-Nov-2019
            craig.trim@ibm.com
            *   removed bidirectionality in generation
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1442
        Updated:
            13-Dec-2019
            craig.trim@ibm.com
            *   add ontology-name as a param
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1583
        Updated:
            14-Jan-2020
            craig.trim@ibm.com
            *   return dictionary from process method for use in synonym generation
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1734
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._ontology_name = ontology_name

    def _owlg(self):
        return OwlGraphConnector(is_debug=self._is_debug,
                                 ontology_name=self._ontology_name).process()

    def process(self) -> dict:
        from taskmda.mda.dto import KbNames
        from taskmda.mda.dto import KbPaths
        from taskmda.mda.dmo import GenericTemplateAccess
        from taskmda.mda.dmo import OntologySeeAlsoGenerator

        d_seealso = OntologySeeAlsoGenerator(self._owlg()).process()

        the_json_result = pprint.pformat(d_seealso, indent=4)
        the_json_result = "{0} = {{\n {1}".format(
            KbNames.seealso(), the_json_result[1:])

        the_template_result = GenericTemplateAccess.process()
        the_template_result = the_template_result.replace(
            "CONTENT", the_json_result)

        path = os.path.join(os.environ["CODE_BASE"],
                            KbPaths.seealso(ontology_name=self._ontology_name))
        FileIO.text_to_file(path, the_template_result)

        return d_seealso
