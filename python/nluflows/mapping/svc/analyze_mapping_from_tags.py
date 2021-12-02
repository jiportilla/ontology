#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from datadict import the_mapping_rev_dict
from datadict import the_mapping_table_dict


class AnalyzeMappingFromTags(BaseObject):
    """
    Purpose:
        iterates through reverse map,
            and using each user tag as a key,
            finds any candidate process
        Note:
            a candidate process has at least one match to a user tag    
    """

    _l_analysis = {}

    def __init__(self,
                 some_tags: list,
                 is_debug: bool = False):
        """
        Updated:
            21-Jun-2017
            craig.trim@ibm.com
            *   inject the mapping table in support of
                https://github.ibm.com/abacus-implementation/Abacus/issues/1624
        Updated:
            6-Feb-2017
            craig.trim@ibm.com
            *   I don't understand why results were being intersected
                I altered logic to use the union operator constantly
        Updated:
            17-Jul-2019
            craig.trim@ibm.com
            *   migrated from abacus-att
                git@github.ibm.com:abacus-implementation/abacus-att.git
            *   add strict typing, debug param and more logging
        :param some_mapping:
            the mapping table to use
        :param some_tags:
            the tags to apply to the mapping table
        """
        BaseObject.__init__(self, __name__)

        self.is_debug = is_debug
        self.tags = set(some_tags)

        self._d_fwd_map = the_mapping_table_dict
        self._d_rev_map = the_mapping_rev_dict

        self._process()

    def get_candidate_matches(self):
        """
        :return:
            a set of candidate processes
        """
        candidates = set()

        for tag in self.tags:
            if tag in self._d_rev_map:
                candidates = candidates.union(set(self._d_rev_map[tag]))
            else:
                self.logger.warn("\n".join([
                    "Unmapped NLU Tag (name={})".format(tag)]))

        return candidates

    def analyze_candidate_matches(self,
                                  some_candidates):
        """
        :param some_candidates:
        :return:
            a list containing an analysis of each candidate matche
        """
        from nluflows.mapping.dmo import TagAnalyzer

        analyses = []
        for candidate_name in some_candidates:
            candidate_entry = self._d_fwd_map[candidate_name]
            analyses.append(TagAnalyzer(self.tags,
                                        candidate_entry,
                                        candidate_name).process())

        return analyses

    def _process(self) -> None:
        if not self.tags or not len(self.tags):
            self._l_analysis = []
            return

        candidates = self.get_candidate_matches()
        if not candidates or not len(candidates):
            self.logger.warn("\n".join([
                "No Candidate Matches Found",
                "\t{}".format(list(self.tags))]))
            self._l_analysis = []
            return

        self._l_analysis = self.analyze_candidate_matches(candidates)

    def analysis(self) -> list:
        return self._l_analysis
