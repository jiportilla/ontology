#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from datagraph import OwlGraphConnector


class EntityLabelGenerator(BaseObject):
    """ Find the label forms of entities from
        1.  The OWL Ontology (Cendant)
        2.  The 'Go Words'
    """

    def __init__(self,
                 ontology_name: str,
                 is_debug: bool = False):
        """
        Created:
            26-Mar-2019
            craig.trim@ibm.com
        Updated:
            1-Apr-2019
            craig.trim@ibm.com
            *   reflect changes in 'ontology-label-generator'
        Updated:
            16-Jul-2019
            craig.trim@ibm.com
            *   removed go-words (file was empty)
            *   added 'see-also' entities
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/444
        Updated:
            16-Aug-2019
            craig.trim@ibm.com
            *   fix a defect in see-also CSV lists identified here
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/773#issuecomment-13922558
        Updated:
            13-Dec-2019
            craig.trim@ibm.com
            *   add ontology-name as a param
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1583
        """
        BaseObject.__init__(self, __name__)
        from . import OntologyLabelGenerator

        self._is_debug = is_debug
        self._ontology_name = ontology_name
        self.label_generator = OntologyLabelGenerator(self._owlg())

    def _owlg(self):
        return OwlGraphConnector(is_debug=self._is_debug,
                                 ontology_name=self._ontology_name).process()

    def _see_also(self) -> set:
        """
        Purpose:
            Retrieve the object of the rdfs:seeAlso property
            this value may be comma-delimited
        :return:
            a set of values
        """
        s = set()

        for label in self.label_generator.see_also().keys():
            if "," in label:
                [s.add(x.strip()) for x in label.split(",") if x]
            else:
                s.add(label.strip())

        return s

    def _labels(self) -> set:
        """
        Purpose:
            Retrieve the object of the rdfs:label property
        :return:
            a set of values
        """
        return set(self.label_generator.labels().keys())

    def process(self) -> list:
        d_labels = {}

        labels = sorted(self._labels().union(self._see_also()))
        for label in labels:
            d_labels[label.lower().strip()] = label

        self.logger.debug("\n".join([
            "Generated Labels (totals={})".format(
                len(d_labels))]))

        return sorted(d_labels.values())
