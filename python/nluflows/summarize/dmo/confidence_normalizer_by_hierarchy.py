#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# pylint:disable=trailing-whitespace


import pprint

from base import BaseObject
from datadict import FindFlowHierarchy


class ConfidenceNormalizerByHierarchy(BaseObject):
    """
    Purpose:
        Given some input

            100:    [   UC4_SC5_DESKTOP_HARDWARE,
                        UC4_SC5_DESKTOP_HARDWARE_CANNOT_LOGIN   ]

        per the flow taxonomy
            UC4_SC5_DESKTOP_HARDWARE_CANNOT_LOGIN
        is a child of
            UC4_SC5_DESKTOP_HARDWARE
        and is thus more specific

        Confidence will be deducted for "parent" level flows
    """

    def __init__(self, some_summary):
        """
        Created:
            14-Mar-2017
            craig.trim@ibm.com
        Updated:
            30-Mar-2017
            craig.trim@ibm.com
            *   renamed from 'FlowTaxonomyConfidenceNormalizer'
        Updated:
            8-Feb-2018
            craig.trim@ibm.com
            *   the primary algorithm was fundamentally flawed
                https://gain-jira-prd2.gain.sl.edst.ibm.com:8443/browse/AGP-2641
        Updated:
            26-Feb-2018
            xavier.verges@es.ibm.com
            *   check for ancestors, not just for parents
            *   deleted commented out code
        Updated:
            13-Jul-2018
            craig.trim@ibm.com
            *   only check for ancestors if multiple flows exist
                -   added '_total_flows'
                -   in support of
                    https://gain-jira-prd2.gain.sl.edst.ibm.com:8443/browse/AGP-6113
            *   moved logic out of 'methods' section
        Updated:
            17-Jul-2019
            craig.trim@ibm.com
            *   migrated from abacus-att
                git@github.ibm.com:abacus-implementation/abacus-att.git
        :param some_summary:
        """
        BaseObject.__init__(self, __name__)
        self.summary = some_summary

    def _total_flows(self):
        """
        :return:
            a list of all the flows in the summary

            "summary" structure:
                {   30.0: ['FLOW_1', 'FLOW_2'], 
                    40.0: ['FLOW_3']
                }

            return this
                [FLOW_1, FLOW_2, FLOW_3]
        """
        return [item for sublist in [self.summary[x]
                                     for x in self.summary] for item in sublist]

    def _ancestor_flows(self):

        ancestors = set()

        for k in self.summary:
            flows = self.summary[k]

            if len(flows) < 2:  # need at least two flows
                continue

            for flow_1 in flows:
                for flow_2 in flows:
                    if flow_1 == flow_2:  # don't compare equivalent flows
                        continue

                    if FindFlowHierarchy(flow_1).is_ancestor_of(flow_2):
                        ancestors.add(flow_1)
                    elif FindFlowHierarchy(flow_2).is_ancestor_of(flow_1):
                        ancestors.add(flow_2)

        return ancestors

    def _modify_summary(self, parent_flows):
        d_modified = {}

        for key in self.summary:
            for flow in self.summary[key]:
                if flow in parent_flows:

                    _key = key - 10
                    if _key not in d_modified:
                        d_modified[_key] = set()
                    d_modified[_key].add(flow)

                else:

                    if key not in d_modified:
                        d_modified[key] = set()
                    d_modified[key].add(flow)

        return d_modified

    def process(self):

        total_flows = self._total_flows()
        if len(total_flows) <= 1:
            return self.summary

        ancestor_flows = self._ancestor_flows()

        if len(ancestor_flows) == 0:
            self.logger.debug("\n".join([
                "No Flow Taxonomy Located",
                pprint.pformat(self.summary, indent=4)]))

            return self.summary

        modified_summary = self._modify_summary(ancestor_flows)

        self.logger.debug("\n".join([
            "Generated Modified Flow Taxonomy",
            pprint.pformat(modified_summary, indent=4)]))

        return modified_summary
