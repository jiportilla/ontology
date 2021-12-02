#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import os
import pprint

from base import BaseObject
from base import FileIO
from base import MandatoryParamError


class GenerateEntityNgrams(BaseObject):
    def __init__(self,
                 ontology_name: str,
                 some_labels: list,
                 some_patterns: dict,
                 is_debug: bool = False):
        """
        Updated:
            1-Aug-2017
            craig.trim@ibm.com
            *   added output-file param so this can be controlled by orchestrating service
                <https://github.ibm.com/abacus-implementation/Abacus/issues/1721#issuecomment-3069168>
        Updated:
            21-Feb-2019
            craig.trim@ibm.com
            *   migrated to text
        Updated:
            28-Mar-2019
            craig.trim@ibm.com
            *   updated to add labels and patterns
        Updated:
            13-Dec-2019
            craig.trim@ibm.com
            *   add ontology-name as a param
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1583
        """
        BaseObject.__init__(self, __name__)
        if not some_labels:
            raise MandatoryParamError("Labels")
        if not some_patterns:
            raise MandatoryParamError("Patterns")

        self._is_debug = is_debug
        self._labels = some_labels
        self._patterns = some_patterns
        self._ontology_name = ontology_name

    def process(self):
        from taskmda.mda.dmo import GenericTemplateAccess
        from taskmda.mda.dmo import EntityNgramDictGenerator
        from taskmda.mda.dto import KbNames
        from taskmda.mda.dto import KbPaths

        dictionary = EntityNgramDictGenerator().process(self._labels,
                                                        list(self._patterns.values()))

        the_json_result = pprint.pformat(dictionary, indent=4)
        the_json_result = "{0} = {{\n {1}".format(
            KbNames.entity_ngrams(), the_json_result[1:])

        the_template_result = GenericTemplateAccess.process()
        the_template_result = the_template_result.replace(
            "CONTENT", the_json_result)

        path = os.path.join(os.environ["CODE_BASE"],
                            KbPaths.ngrams(ontology_name=self._ontology_name))
        FileIO.text_to_file(path, the_template_result)

        return dictionary
