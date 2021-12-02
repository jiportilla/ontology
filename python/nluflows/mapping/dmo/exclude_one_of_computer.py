#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from base import BaseObject


class ExcludeOneOfComputer(BaseObject):
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
            each mapping has an (optional) 'exclude-one-of'; 
            -   if at least ONE tag in this segment is located, the entire mapping should be penalized

        :return: some value [0-100] that represents a confidence deduction
            due to a partial mapping
        """

        def _has_mapping():
            if "exclude_one_of" not in self.analyses["analysis"]:
                return False
            if not len(self.analyses["analysis"]):
                return False
            return True

        # no penalty if the segment is not found
        if not _has_mapping():
            if self.is_debug:
                self.logger.debug("\n".join([
                    "Exclude-One-Of Rule",
                    "\tName: No Mapping",
                    "\tPenalty: 0"]))
            return 0

        segment = self.analyses["analysis"]["exclude_one_of"]
        contains = float(len(segment["contains"]))

        # no exclusion tags found
        if contains == 0:
            if self.is_debug:
                self.logger.debug("\n".join([
                    "Exclude-One-Of Rule",
                    "\tName: No Exclusions Found",
                    "\tPenalty: 0"]))
            return 0

        # penalty is severe
        total = contains * -75
        if self.is_debug:
            self.logger.debug("\n".join([
                "Exclude-One-Of Rule",
                "\tName: Exclusions Match",
                "\tPenalty: {}".format(total)]))
        return total
