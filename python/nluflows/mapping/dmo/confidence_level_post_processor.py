#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from base import BaseObject


class ConfidenceLevelPostProcessor(BaseObject):
    def __init__(self,
                 some_analyses: list,
                 is_debug: bool = False):
        """
        Create:
            7-Apr-2017
            craig.trim@ibm.com
            *   refactored out of svc:ComputeConfidenceLevels
        Updated:
            17-Jul-2019
            craig.trim@ibm.com
            *   migrated from abacus-att
                git@github.ibm.com:abacus-implementation/abacus-att.git
        :param some_analyses:
        """
        BaseObject.__init__(self, __name__)
        self.analyses = some_analyses
        self.is_debug = is_debug

    def get_max_confidence(self):
        """
        :return: the maximum confidence among all analysis results
        """
        max_confidence = 0
        for analysis in self.analyses:
            if analysis["confidence"] > max_confidence:
                max_confidence = analysis["confidence"]

        return max_confidence

    def fit_curve(self):
        """
        Purpose:
            poor-mans sigmoid curve fitting
        Example:
            given four analysis results with these confidence levels
                [115, 100, 80, 10]
            normalize to
                [100, 85, 65, 0]
            note how the delta is computed from the highest result
                delta = -15
            and the delta is deducted from each result
        :return: normalized confidence levels
        """

        # determine the delta
        max_confidence = self.get_max_confidence()
        delta = 0
        if max_confidence > 100:
            delta = self.get_max_confidence() - 100

        # deduct the delta from each analysis object
        for analysis in self.analyses:
            analysis["confidence"] -= delta

            # no confidence level shall be lower than 0
            if analysis["confidence"] < 0:
                analysis["confidence"] = 0

    def tie_break(self):
        """
        Purpose:
            for two equal flows, 
            gives a boost to the flows with the most "include-all-of" tags
        :return:
        """
        max_confidence = self.get_max_confidence()
        for analysis in self.analyses:
            if max_confidence != analysis["confidence"]:
                continue

            total_include_all_of = len(analysis["analysis"]["include_all_of"]["total"])
            analysis["confidence"] += total_include_all_of * 5

    def process(self):
        self.tie_break()
        self.fit_curve()

        return self.analyses
