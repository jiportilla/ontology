#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from base import BaseObject


class HighMatchComputer(BaseObject):
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
            1.  if a mapping exist that contains an empty 'include-all-of' section
                then this is mapping is very imprecise and a deduction will exist for that
            2.  if a mappping exists that contains an 'include-all-of' section
                a confidence boost will occur for each match
                
                Example:
                    mapping-1
                        include-all-of [alpha, beta, gamma, delta]
                        
                    mapping-2
                        include-all-of [alpha]
                        
                    UPS:
                        [alpha, beta, gamma, delta]
                            
                    This UPS will match both
                        mapping-1
                        mapping-2
                        
                    but higher confidence should be granted to mapping-1
                    since more tags were matched 
    :return: 
        """
        contains = self.analyses["analysis"]["include_all_of"]["contains"]
        total_include_all_of = len(contains)

        if total_include_all_of > 0:
            total = total_include_all_of * 5
            if self.is_debug:
                self.logger.debug("\n".join([
                    "High Match Rule",
                    "\tName: Boost",
                    "\tScore: {}".format(total)]))
            return total

        if total_include_all_of == 0:
            if self.is_debug:
                self.logger.debug("\n".join([
                    "High Match Rule",
                    "\tName: Full Inclusion Not Found",
                    "\tPenalty: -25"]))
            return -25

        if self.is_debug:
            self.logger.debug("\n".join([
                "High Match Rule",
                "\tName: Fall Through",
                "\tPenalty: 0"]))

        return 0
