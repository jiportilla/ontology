#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import re

from base import BaseObject


class FlowNameNormalizer(BaseObject):
    def __init__(self,
                 some_summarized_mapping):
        """
        given sample input:
            {   67: ['UC4_SC5_NETWORK_DRIVE_1'],
                50: ['UC4_SC5_NETWORK_DRIVE_2'],
                40: ['UC1_TOKEN_ISSUE_VDI_FROM_CLASS'],
                0: ['UC2_SC3_GOOD_WORK_INTERPRETED_AS_2']}
        return:
            {   67: ['UC4_SC5_NETWORK_DRIVE'],
                50: ['UC4_SC5_NETWORK_DRIVE'],
                40: ['UC1_TOKEN_ISSUE_VDI_FROM_CLASS'],
                0: ['UC2_SC3_GOOD_WORK_INTERPRETED_AS']}
        Updated:
            17-Jul-2019
            craig.trim@ibm.com
            *   migrated from abacus-att
                git@github.ibm.com:abacus-implementation/abacus-att.git
        :param some_summarized_mapping:
        """
        BaseObject.__init__(self, __name__)
        self.summarized_mapping = some_summarized_mapping

    @staticmethod
    def normalize_flow_name(some_flow):
        ctr = 0

        # only perform normalization if there is an underscore
        # within the last four chars of the flow name
        if "_" not in some_flow[len(some_flow) - 4:]:
            return some_flow

        some_flow = re.sub("_(\d+)$", "", some_flow)

        return some_flow

    def normalize_flow_names(self, some_flows):
        normalized_flows = set()
        for flow in some_flows:
            normalized_flows.add(self.normalize_flow_name(flow))
        return list(normalized_flows)

    def process(self):
        ndict = {}

        for key in self.summarized_mapping:
            ndict[key] = self.normalize_flow_names(self.summarized_mapping[key])

        return ndict
