#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from base import BaseObject


class FlagComputer(BaseObject):
    def __init__(self,
                 some_analyses: dict,
                 is_debug: bool = False):
        """
        Created:
            17-Jul-2019
            craig.trim@ibm.com
            *   refactored out of 'compute-confidence-levels'
        :param some_analyses:
        """
        BaseObject.__init__(self, __name__)
        self.analyses = some_analyses
        self.is_debug = is_debug

    def _manage_deductions(self) -> float:
        """
        Manage the Deduction Flag
        :return:
            a Confidence Score Penalty (or boost)
        """
        total = self.analyses["analysis"]["flags"]["deduction"]
        if self.is_debug:
            self.logger.debug("\n".join([
                "Deduction Rule",
                "\tName: Match",
                "\tPenalty: {}".format(total)]))

        return total

    def _manage_discrimin_flag(self) -> float:
        """
        Manage the Discriminatory Flag
        :return:
            a Confidence Score Penalty (or boost)
        """
        total = (self.analyses["analysis"]["flags"]["discriminatory"] * 10)
        if self.is_debug:
            self.logger.debug("\n".join([
                "Discriminatory Flag Rule",
                "\tName: Match",
                "\tPenalty: {}".format(total)]))

        return total

    def process(self) -> float:
        total = 0

        total -= self._manage_deductions()
        total += self._manage_discrimin_flag()

        return total
