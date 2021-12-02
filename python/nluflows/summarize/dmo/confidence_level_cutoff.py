#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject


class ConfidenceLevelCutoff(BaseObject):
    def __init__(self, some_summary):
        """
        Purpose:
            30-Mar-2017
            craig.trim@ibm.com
            *   extrapolated out of 'PerformServiceCatalogMapping' in orchestrator
            *   any flow with a confidence level < THRESHOLD
                will be completely removed from the result set
        Updated:
            17-Jul-2019
            craig.trim@ibm.com
            *   migrated from abacus-att
                git@github.ibm.com:abacus-implementation/abacus-att.git
        :param some_summary:
        """
        BaseObject.__init__(self, __name__)
        self.summary = some_summary

    def filter_flows_by_thresholds(self, min_threshold=50):
        ndict = {}

        for key in self.summary:
            if key >= min_threshold:
                ndict[key] = self.summary[key]

        return ndict

    def process(self):
        return self.filter_flows_by_thresholds()
