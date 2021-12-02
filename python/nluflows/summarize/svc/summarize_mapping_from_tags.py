#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject


class SummarizeMappingFromTags(BaseObject):
    def __init__(self, some_analyses):
        """
        Updated:
            17-Jul-2019
            craig.trim@ibm.com
            *   migrated from abacus-att
                git@github.ibm.com:abacus-implementation/abacus-att.git
        :param some_analyses:
        """
        BaseObject.__init__(self, __name__)
        self.analyses = some_analyses

    @staticmethod
    def normalize(some_dict):
        ndict = {}
        for key in some_dict:
            ndict[key] = list(some_dict[key])
        return ndict

    def process(self):
        from nluflows.summarize.dmo import AnalysisSummaryGenerator
        from nluflows.summarize.dmo import ConfidenceNormalizerByValue
        from nluflows.summarize.dmo import ConfidenceNormalizerByDictionary
        from nluflows.summarize.dmo import ConfidenceNormalizerByHierarchy
        from nluflows.summarize.dmo import ConfidenceNormalizerByAbsoluteFlow
        from nluflows.summarize.dmo import FlowNameNormalizer
        from nluflows.summarize.dmo import ConfidenceLevelCutoff
        from nluflows.summarize.dmo import MultiFlowNormalizer

        # create a summarized form of the flow result
        summary = AnalysisSummaryGenerator(self.analyses).process()
        if len(summary) == 0:
            return summary

        # split multi-flows as single entries into multiple entries
        summary = MultiFlowNormalizer(summary).process()
        if len(summary) == 0:
            return summary

        # remove the trailing "_N" from each flow (where present)
        summary = FlowNameNormalizer(summary).process()
        if len(summary) == 0:
            return summary

        # if same flow occurs twice; remove lower confidence form
        summary = ConfidenceNormalizerByValue(summary).process()
        if len(summary) == 0:
            return summary

        # specialized confidence level reduction (dictionary driven)
        summary = ConfidenceNormalizerByDictionary(summary).process()
        if len(summary) == 0:
            return summary

        # lower confidence of a parent flow when a child flow exists
        summary = ConfidenceNormalizerByHierarchy(summary).process()
        if len(summary) == 0:
            return summary

        # lower confidence of a flow when it occurs in the context of any other flow
        summary = ConfidenceNormalizerByAbsoluteFlow(summary).process()
        if len(summary) == 0:
            return summary

        # # lower confidence of a flow when it occurs in the context of a
        # # specific flow
        # summary = ConfidenceNormalizerByRelativeFlow(summary).process()
        # if len(summary) == 0:
        #     return summary

        # completely remove any flows below the threshold
        summary = ConfidenceLevelCutoff(summary).process()
        return summary
