#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pprint

from base import BaseObject


class ServiceCatalogSummarizer(BaseObject):
    """
    Perform Service Catalog Summarization
    """

    def __init__(self,
                 some_analyses: dict,
                 is_debug: bool = False):
        """
        Created:
            17-Jul-2019
            craig.trim@ibm.com
            *   refactored out of 'abacus-mapping-api'
        :param some_tags:
            the tags that will be used in the mapping
        """
        BaseObject.__init__(self, __name__)

        self.analysis = some_analyses
        self.is_debug = is_debug

    def process(self):
        from nluflows.summarize.svc import SummarizeMappingFromTags

        summary = SummarizeMappingFromTags(self.analysis).process()
        if 0 != len(summary) and self.is_debug:
            self.logger.debug("\n".join([
                "Summarization Completed",
                pprint.pformat(summary, indent=4)]))

        return summary
