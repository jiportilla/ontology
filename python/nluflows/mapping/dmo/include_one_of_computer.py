#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from base import BaseObject


class IncludeOneOfComputer(BaseObject):
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
            each mapping has an (optional) 'include-one-of';
            at least ONE tag in this segment MUST BE located
            - the penalty for not matching any tag in this segment is severe
            - there is a confidence boost for matching multiple tags in this segment 

        :return: some value [0-100] that represents a confidence deduction
            due to a partial mapping
        """

        def _has_mapping():
            if "include_one_of" not in self.analyses["analysis"]:
                return False
            if not len(self.analyses["analysis"]):
                return False
            return True

        # no penalty if the segment is not found
        if not _has_mapping():
            if self.is_debug:
                self.logger.debug("\n".join([
                    "Include-One-Of Rule",
                    "\tName: Early Exit",
                    "\tPenalty: 0"]))
            return 0

        segment = self.analyses["analysis"]["include_one_of"]
        total = float(len(segment["total"]))
        contains = float(len(segment["contains"]))

        if total > 0 and contains == 0:
            if self.is_debug:
                self.logger.debug("\n".join([
                    "Include-One-Of Rule",
                    "\tName: No Match",
                    "\tPenalty: -75"]))
            return -75

        # confidence boost 5 points per inclusion match found
        # only invoked if multiple tags are found
        if contains > 0:

            total = contains * 5.0
            if self.is_debug:
                self.logger.debug("\n".join([
                    "Include-One-Of Rule",
                    "\tName: Confidence Boost",
                    "\tBoost: {}".format(total)]))
            return total

        if self.is_debug:
            self.logger.debug("\n".join([
                "Include-One-Of Rule",
                "\tName: Fall Through",
                "\tPenalty: 0"]))
        return 0
