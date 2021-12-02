#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import time

from base import BaseObject


class MdaOrchestrator(BaseObject):
    """
    """

    def __init__(self,
                 is_debug: bool = False):
        """
        Created:
            20-Feb-2019
            craig.trim@ibm.com
        Updated:
            11-Apr-2019
            craig.trim@ibm.com
            *   injected synonyms output from 'patterns' generator
                into 'synonyms' generator
        Updated:
            14-Aug-2019
            craig.trim@ibm.com
            *   added task timer
        Updated:
            13-Dec-2019
            craig.trim@ibm.com
            *   add ontology-name as a param
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1583
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug

    def process(self,
                action: str):

        from taskmda import GenerateStopwordsDict
        from taskmda import GenerateReferences
        from taskmda import GenerateCountryLookup
        from taskmda import GenerateMappingTable
        from taskmda import GenerateFlowTaxonomy
        from taskmda import GenerateRegionLookup
        from taskmda import GenerateCertificationsLookup
        from taskmda import GenerateCertificationsHierarchy
        from taskmda import GenerateDimDictionaries
        from taskmda import GenerateOntologyDictionaries

        start = time.time()

        if action == "all":
            GenerateMappingTable(is_debug=self._is_debug).process()
            GenerateStopwordsDict(is_debug=self._is_debug).process()
            GenerateFlowTaxonomy(is_debug=self._is_debug).process()
            GenerateCertificationsLookup(is_debug=self._is_debug).process()
            GenerateCertificationsHierarchy(is_debug=self._is_debug).process()
            GenerateDimDictionaries(is_debug=self._is_debug).process()

            # Generate Biotech Dictionaries
            GenerateOntologyDictionaries(syns_only=False,
                                         ontology_name='biotech',
                                         is_debug=self._is_debug).process()

            # Generate Cendant (base) Dictionaries
            GenerateOntologyDictionaries(syns_only=False,
                                         ontology_name='base',
                                         is_debug=self._is_debug).process()

        elif action.startswith("bio"):
            GenerateOntologyDictionaries(syns_only=False,
                                         ontology_name='biotech',
                                         is_debug=self._is_debug).process()

        elif action.startswith("syn"):

            # Generate Biotech Synonyms
            GenerateOntologyDictionaries(syns_only=True,
                                         ontology_name='biotech',
                                         is_debug=self._is_debug).process()

            # Generate Cendant (base) Synonyms
            GenerateOntologyDictionaries(syns_only=True,
                                         ontology_name='base',
                                         is_debug=self._is_debug).process()


        elif action.startswith("geo"):
            GenerateCountryLookup(is_debug=self._is_debug).process()
            GenerateRegionLookup(is_debug=self._is_debug).process()

        elif action.startswith("dim"):
            GenerateDimDictionaries(is_debug=True).process()

        elif action.startswith("wiki"):
            GenerateReferences(is_debug=self._is_debug).process()

        print(f"Task Completion Time: "
              f"{round((time.time() - start) / 60, 2)}m")


def main(some_action):
    if not some_action:
        some_action = "all"
    MdaOrchestrator(is_debug=True).process(some_action)


if __name__ == "__main__":
    import plac

    plac.call(main)
