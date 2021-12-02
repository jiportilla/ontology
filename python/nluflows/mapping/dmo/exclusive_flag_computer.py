#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from base import BaseObject


class ExclusiveFlagComputer(BaseObject):
    def __init__(self,
                 some_analyses: dict,
                 is_debug: bool = False):
        """
        Create:
            6-Apr-2017
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

    def process(self):
        """
        Purpose:
            each mapping has an (optional) 'include-all-of' field;
            ALL tags in this segment MUST BE located
            the penalty for missing a single tag in this segment is severe

        :return:
            some value [ -100 <= x <= 100 ]
            that represents a confidence change
        """

        requires_exclusivity = self.analyses["analysis"]["exclusivity"]["required"]
        has_exclusivity = self.analyses["analysis"]["exclusivity"]["matched"]

        if requires_exclusivity and not has_exclusivity:
            if self.is_debug:
                self.logger.debug("\n".join([
                    "Exclusive Rule",
                    "\tName: Exclusivity Required",
                    "\tPenalty: -75"]))
            return -75

        if requires_exclusivity and has_exclusivity:
            if self.is_debug:
                self.logger.debug("\n".join([
                    "Exclusive Rule",
                    "\tName: Exclusivity Maintained",
                    "\tPenalty: 5"]))
            return 5

        if self.is_debug:
            self.logger.debug("\n".join([
                "Exclusive Rule",
                "\tName: Fall Through",
                "\tPenalty: 0"]))

        return 0
