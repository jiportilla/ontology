#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from spacy.tokens import Doc

from base import BaseObject
from nlutag.core.dmo import EntityToTagGenerator


class PerformDeepNLU(BaseObject):
    """
    Purpose:
        Given the preprocessed UPS structure, perform
        1.  tagging (aka annotation) via CitiProductAnnotator
        2.  inference via CitiInference
    """

    def __init__(self,
                 ontology_name: str,
                 is_debug: bool = False):
        """
        Created:
            13-Feb-2017
            craig.trim@ibm.com
            *   extrapolated out of an increasingly complex function within pipeline_orchestrator.py
        Updated:
            28-Feb-2017
            craig.trim@ibm.com
            *   replaced single dict param with individual parameters
                this makes service design more consistent
        Updated:
            24-Mar-2017
            craig.trim@ibm.com
            -   sending 'normalized' instead of 'original' now
            -   supporting <928#issuecomment-1947787> and potentially other defects
        Updated:
            12-Mar-2019
            craig.trim@ibm.com
            *   migrate to text and remove dead code
        Updated:
            25-Apr-2019
            craig.trim@ibm.com
            *   add strict typing and more logging statements with is-debug flag
            *   change instantiation strategy
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._ontology_name = ontology_name

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Instantiate PerformDeepNLU",
                f"\tOntology Name: {self._ontology_name}"]))

    @staticmethod
    def _to_structure(some_original_ups: str,
                      some_normalized_ups: str,
                      some_tokenation_result: dict) -> dict:
        """
        Purpose:
            combine normalization+tokenization into a single data structure
        Notes:
            there's no real reason for this except that this service
            was re-factored out of a messy business process and the
            domain components below require this structure as input
        :param some_original_ups: 
            the original UPS
        :param some_normalized_ups: 
            the normalized UPS
        :param some_tokenation_result: 
            the tokens to perform inference on
        :return:
        """
        return {
            "original": some_original_ups,
            "normalized": some_normalized_ups,
            "tokens": some_tokenation_result}

    def process(self,
                doc: Doc,
                some_original_ups: str,
                some_normalized_ups: str,
                some_tokenation_result: dict):
        """
        :return:
            normalized matches (aka entities, tags)
        """
        from nlutag.core.svc import AbacusAnnotator

        preprocessed = self._to_structure(some_original_ups,
                                          some_normalized_ups,
                                          some_tokenation_result)

        # Step: Execute the annotation pipeline
        annotator = AbacusAnnotator(doc=doc,
                                    d_input=preprocessed,
                                    is_debug=self._is_debug,
                                    ontology_name=self._ontology_name)

        the_matches = annotator.process()

        the_tags = EntityToTagGenerator(self._is_debug).process(the_matches)

        return None, the_tags
