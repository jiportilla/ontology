#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from base import BaseObject


class IncludeAllOfComputer(BaseObject):
    def __init__(self,
                 some_tags: list,
                 some_analyses: dict,
                 is_debug: bool = False):
        """
        Create:
            3-Apr-2017
            craig.trim@ibm.com
            *   refactored out of dmo:ConfidenceLevelComputer
        Updated:
            17-Jul-2019
            craig.trim@ibm.com
            *   migrated from abacus-att
                git@github.ibm.com:abacus-implementation/abacus-att.git
        :param some_analyses:
        """
        BaseObject.__init__(self, __name__)
        self.tags = some_tags
        self.analyses = some_analyses
        self.is_debug = is_debug

    def process(self):
        """
        Purpose:
            each mapping has an (optional) 'include-all-of' field; ALL tags in this segment MUST BE located
            the penalty for missing a single tag in this segment is severe

        :return: some value [0-100] that represents a confidence deduction
            due to a partial mapping
        """

        def _has_mapping():
            if "include_all_of" not in self.analyses["analysis"]:
                return False
            if not len(self.analyses["analysis"]):
                return False
            return True

        # no penalty if the segment is not found
        if not _has_mapping():
            if self.is_debug:
                self.logger.debug("\n".join([
                    "Include-All-Of Rule",
                    "\tName: Early Exit",
                    "\tPenalty: 0"]))
            return 0

        # find the segment and required fields
        segment = self.analyses["analysis"]["include_all_of"]
        missing = float(len(segment["missing"]))

        # no penalty if nothing is missing
        if missing == 0:
            if self.is_debug:
                self.logger.debug("\n".join([
                    "Include-All-Of Rule",
                    "\tName: Nothing Missing",
                    "\tPenalty: 0"]))
            return 0

        # for each missing element, deduct a large penalty
        total = missing * -100
        if self.is_debug:
            self.logger.debug("\n".join([
                "Include-All-Of Rule",
                "\tName: Missing Elements",
                "\tPenalty: {}".format(total)]))

        return total
