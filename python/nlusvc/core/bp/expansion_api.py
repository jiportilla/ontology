#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from pandas import DataFrame

from base import BaseObject


class ExpansionAPI(BaseObject):

    def __init__(self,
                 is_debug: bool = False):
        """
        Created:
            27-Jun-2019
            craig.trim@ibm.com
            *   designed for the purpose of extracting acronyms and abbreviations from text
        """
        BaseObject.__init__(self, __name__)

        self.is_debug = is_debug

    def acronyms(self,
                 source_text: str):
        return self.all(source_text=source_text,
                        include_acronyms=True,
                        include_instances=False)

    def instances(self,
                  source_text: str):
        return self.all(source_text=source_text,
                        include_acronyms=False,
                        include_instances=True)

    def all(self,
            source_text: str,
            include_acronyms: bool = True,
            include_instances: bool = True):
        from nlusvc.core.svc import ExtractAcronymExpansions
        extractor = ExtractAcronymExpansions(source_text=source_text,
                                             include_acronyms=include_acronyms,
                                             include_instances=include_instances,
                                             is_debug=self.is_debug)
        return extractor.process()

    def explain(self,
                expansions: DataFrame) -> DataFrame:
        from nlusvc.core.svc import ExplainAcronymExpansions

        explainer = ExplainAcronymExpansions(expansions=expansions,
                                             is_debug=self.is_debug)
        return explainer.process()
