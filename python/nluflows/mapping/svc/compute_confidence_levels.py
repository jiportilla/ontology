#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from base import BaseObject


class ComputeConfidenceLevels(BaseObject):
    _d_analysis = {}

    def __init__(self,
                 some_tags: list,
                 some_analyses: list,
                 is_debug: bool = False):
        """
        Updated:
            3-Apr-2017
            craig.trim@ibm.com
            *   major computation updates around inclusion deductions
                reference SEV-1 #1343
            *   migrated to a service from dmo:ConfidenceLevelComputer
        Updated:
            17-Jul-2019
            craig.trim@ibm.com
            *   migrated from abacus-att
                git@github.ibm.com:abacus-implementation/abacus-att.git
        :param some_analyses:
        """
        BaseObject.__init__(self, __name__)
        self.is_debug = is_debug

        self._tags = some_tags
        self._process(some_analyses)

    def _manage_deductions(self,
                           d_analysis: dict) -> float:
        """
        Manage the Deduction Flag
        :param d_analysis:
        :return:
            a Confidence Score Penalty (or boost)
        """
        total = d_analysis["analysis"]["flags"]["deduction"]
        if self.is_debug:
            self.logger.debug("\n".join([
                "Deduction Rule",
                "\tName: Match",
                "\tPenalty: {}".format(total)]))

        return total

    def _manage_discrimin_flag(self,
                               d_analysis: dict) -> float:
        """
        Manage the Discriminatory Flag
        :param d_analysis:
        :return:
            a Confidence Score Penalty (or boost)
        """
        total = (d_analysis["analysis"]["flags"]["discriminatory"] * 10)
        if self.is_debug:
            self.logger.debug("\n".join([
                "Discrim Flag Rule",
                "\tName: Match",
                "\tPenalty: {}".format(total)]))

        return total

    def compute_confidence(self,
                           d_analysis: dict):
        from nluflows.mapping.dmo import FlagComputer
        from nluflows.mapping.dmo import ExcludeOneOfComputer
        from nluflows.mapping.dmo import ExcludeAllOfComputer
        from nluflows.mapping.dmo import ExclusiveFlagComputer
        from nluflows.mapping.dmo import HighMatchComputer
        from nluflows.mapping.dmo import IncludeOneOfComputer
        from nluflows.mapping.dmo import IncludeAllOfComputer

        confidence = 100

        # if not is_direct_match:
        confidence += IncludeOneOfComputer(some_tags=self._tags,
                                           some_analyses=d_analysis,
                                           is_debug=self.is_debug).process()
        confidence += IncludeAllOfComputer(some_tags=self._tags,
                                           some_analyses=d_analysis,
                                           is_debug=self.is_debug).process()
        confidence += ExcludeOneOfComputer(some_tags=self._tags,
                                           some_analyses=d_analysis,
                                           is_debug=self.is_debug).process()
        confidence += ExcludeAllOfComputer(some_tags=self._tags,
                                           some_analyses=d_analysis,
                                           is_debug=self.is_debug).process()

        # manage exclusive flag
        confidence += ExclusiveFlagComputer(some_analyses=d_analysis,
                                            is_debug=self.is_debug).process()

        # manage contains (method)
        confidence += HighMatchComputer(some_analyses=d_analysis,
                                        is_debug=self.is_debug).process()

        # manage other mapping flags
        confidence += FlagComputer(some_analyses=d_analysis,
                                   is_debug=self.is_debug).process()

        return confidence

    def _process(self,
                 incoming_analysis: list):
        from nluflows.mapping.dmo import ConfidenceLevelPostProcessor

        for analysis in incoming_analysis:
            analysis["confidence"] = self.compute_confidence(analysis)

        self._d_analysis = ConfidenceLevelPostProcessor(incoming_analysis).process()

    def analysis(self) -> dict:
        return self._d_analysis
