#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject


class ServiceCatalogMapper(BaseObject):
    """
    Perform Service Catalog Mapping
    """

    def __init__(self,
                 some_tags: list,
                 is_debug: bool = False):
        """
        Updated:
            21-Jun-2017
            craig.trim@ibm.com
            *   injection of mapping table via a parameter
                in support of
                    <https://github.ibm.com/abacus-implementation/Abacus/issues/1624>
            *   removed debug parameter (wasn't being used)
        Updated:
            17-Jul-2019
            craig.trim@ibm.com
            *   migrated from abacus-att
                git@github.ibm.com:abacus-implementation/abacus-att.git
            *   default mapping table to 'dict::mapping-table'
            *   refactored to include only 'mapping' logic
                and place summarization and other logic into other packages
        :param some_tags:
            the tags that will be used in the mapping
        """
        BaseObject.__init__(self, __name__)

        self.is_debug = is_debug
        self.tags = [x.lower().strip() for x in some_tags
                     if x]

    def analyze_mapping_from_tags(self) -> list:
        from nluflows.mapping.svc import AnalyzeMappingFromTags

        analyses = AnalyzeMappingFromTags(
            some_tags=self.tags,
            is_debug=self.is_debug).analysis()

        if 0 == len(analyses):
            self.logger.warn("\n".join([
                "Mapping Not Found",
                "\tTags: {}".format(self.tags)]))
            return []

        return analyses

    def compute_confidence_levels(self,
                                  some_analyses: list):
        from nluflows.mapping.svc import ComputeConfidenceLevels

        analyses = ComputeConfidenceLevels(some_tags=self.tags,
                                           some_analyses=some_analyses,
                                           is_debug=self.is_debug).analysis()
        if 0 == len(analyses):
            self.logger.warn("No Mapping Found:\n\t{0}".format(self.tags))
            return {}

        return analyses

    # def in_scope(self):
    #     from abacuscygnuscatalog import AnalyzeTagScope
    #     return AnalyzeTagScope(self.tags).process()

    def process(self) -> dict:
        if 0 == len(self.tags):
            return {}

        # if not self.in_scope():
        #     self.logger.warn("UPS should be searched")
        #     return {}, {100.0: ["OUT_OF_SCOPE"]}

        analyses = self.analyze_mapping_from_tags()
        if 0 == len(analyses):
            return {}

        analyses = self.compute_confidence_levels(analyses)
        if 0 == len(analyses):
            return {}

        return analyses
