#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from datagraph import OwlGraphConnector


class EntityParentGenerator(BaseObject):
    """ Find the label forms of entities from
        1.  The OWL Ontology (Cendant)
        2.  The 'Go Words' (assume 'DomainTerm' for all entries)
    """

    def __init__(self,
                 ontology_name: str,
                 is_debug: bool = False):
        """
        Created:
            26-Mar-2019
            craig.trim@ibm.com
        Updated:
            16-Jul-2019
            craig.trim@ibm.com
            *   removed go-words (file was empty)
        Updated:
            13-Dec-2019
            craig.trim@ibm.com
            *   dynamic loading of Ontology from param
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1583#issuecomment-16612838
        """
        BaseObject.__init__(self, __name__)

        self._is_debug = is_debug
        self._ontology_name = ontology_name

    def _owl_parents(self) -> dict:
        from taskmda.mda.dmo import OntologyParentGenerator
        owlg = OwlGraphConnector(is_debug=self._is_debug,
                                 ontology_name=self._ontology_name).process()
        return OntologyParentGenerator(owlg).process()

    def process(self) -> dict:
        d_parents = self._owl_parents()

        self.logger.debug(f"Generated Parents "
                          f"(total={len(d_parents)})")

        d_normal = {}
        for k in d_parents:
            d_normal[k.lower()] = [x.lower().strip() for x in d_parents[k]]

        return d_normal
