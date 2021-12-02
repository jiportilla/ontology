#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from spacy.lang.en import English

from base import BaseObject
from nlutag import PerformDeepNLU


class PerformSupervisedParsing(BaseObject):
    """ parse manifest data using supervised techniques

        supervised parsing takes a known entity

        for example

            Aix 5.3 Workload:
              type: Software
              scoped: TRUE
              provenance:
                - GTS
              patterns:
                - 'aix_5.3_workload'
                - '5.3+aix+workload'

        and finds these patterns in the unstructured text and if a match is found
        correlates to the known entity tag 'AIX 5.3 Workload'
    """

    __nlp = English()

    def __init__(self,
                 ontology_name: str,
                 is_debug: bool = False):
        """
        Created:
            12-Mar-2019
            craig.trim@ibm.com
            the name of the activity within the manifest
        Updated:
            14-Mar-2019
            craig.trim@ibm.com
            *   renamed from 'svc:perform-supervised-parsing'
        Updated:
            16-May-2019
            craig.trim@ibm.com
            *   add 'is_debug' param
        Updated:
            21-Aug-2019
            craig.trim@ibm.com
            *   eschew the use of tokenizers for spaCy doc instead
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/796
        Updated:
            13-Dec-2019
            craig.trim@ibm.com
            *   pass in ontology name as a param
                https://github.ibm.com/GTS-CDO/unstructured-analytics/pull/1587
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._ontology_name = ontology_name

        if self._is_debug:
            self.logger.debug("Instantiate PerformSupervisedParsing")

    def process(self,
                original_ups: str,
                normalized_ups: str) -> list:
        from nlutext.core.svc import PerformDeepTokenization

        # tokenize the UPS
        doc = self.__nlp(normalized_ups)

        # perform deeper tokenization (includes spell correction)
        deep_tok = PerformDeepTokenization(doc=doc,
                                           is_debug=self._is_debug,
                                           ontology_name=self._ontology_name)
        tokenization_result = deep_tok.process()

        deep_nlu = PerformDeepNLU(is_debug=self._is_debug,
                                  ontology_name=self._ontology_name)

        _, tags = deep_nlu.process(doc=doc,
                                   some_original_ups=original_ups,
                                   some_normalized_ups=normalized_ups,
                                   some_tokenation_result=tokenization_result)

        return [x for x in tags if x]
