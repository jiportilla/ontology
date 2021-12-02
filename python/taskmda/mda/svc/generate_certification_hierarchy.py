# !/usr/bin/env python
# -*- coding: UTF-8 -*-


import os
import pprint

from base import BaseObject
from base import FileIO


class GenerateCertificationsHierarchy(BaseObject):
    """
    Create a Lookup Dictionary that contains a Certification Hierarchy
    for example:
        {   'ITIL Foundation': [    'ITIL Foundation Certification',
                                    'ITIL Master Certification', ... ]}
    """

    def __init__(self,
                 is_debug: bool = False):
        """
        Created:
            6-Aug-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/629
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug

    def process(self):
        from taskmda.mda.dmo import CertificationHierarchyGenerator
        from taskmda.mda.dto import KbNames
        from taskmda.mda.dto import KbPaths
        from taskmda.mda.dmo import GenericTemplateAccess

        svcresult = CertificationHierarchyGenerator().process()

        the_json_result = pprint.pformat(svcresult, indent=4)
        the_json_result = "{0} = {{\n {1}".format(
            KbNames.certs_hierarchy(), the_json_result[1:])

        the_template_result = GenericTemplateAccess.process()
        the_template_result = the_template_result.replace(
            "CONTENT", the_json_result)

        path = os.path.join(os.environ["CODE_BASE"],
                            KbPaths.certs_hierarchy())
        FileIO.text_to_file(path, the_template_result)

        return svcresult
