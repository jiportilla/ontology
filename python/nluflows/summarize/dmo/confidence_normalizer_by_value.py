#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject

# all confidence levels less than the threshold will be ignored
MIN_CONFIDENCE_THRESHOLD = 0


class ConfidenceNormalizerByValue(BaseObject):
    def __init__(self, some_summarized_mapping):
        """
        Purpose:
            given sample input:
                {
                    67: ['UC4_SC5_NETWORK_DRIVE'],
                    50: ['UC4_SC5_NETWORK_DRIVE'],
                    40: ['UC1_TOKEN_ISSUE_VDI_FROM_CLASS'],
                    0: ['UC2_SC3_GOOD_WORK_INTERPRETED_AS']}
            return:
                {
                    67: ['UC4_SC5_NETWORK_DRIVE'],
                    40: ['UC1_TOKEN_ISSUE_VDI_FROM_CLASS']}
        Updated:
            30-Mar-2017
            craig.trim@ibm.com
            *   renamed from 'ConfidenceLevelNormalizer'
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
    def normalize_confidence_level(some_mapping):
        """
        :return:
        """
        ndict = {}
        for key in some_mapping:
            confidence_level = int(key)
            if confidence_level <= MIN_CONFIDENCE_THRESHOLD:
                continue

            ndict[key] = some_mapping[key]
        return ndict

    @staticmethod
    def create_rev_map(some_normalized_mapping_dict):
        """
        :return:
        """
        revmap = {}
        for key in some_normalized_mapping_dict:
            for some_flow in some_normalized_mapping_dict[key]:
                if some_flow not in revmap:
                    revmap[some_flow] = []
                revmap[some_flow].append(key)

        return revmap

    @staticmethod
    def remove_dupe_flows_by_confidence(some_revmap):
        """
        :param some_revmap:
        :return:
        """
        for flow in some_revmap:
            if len(some_revmap[flow]) < 2:
                some_revmap[flow] = some_revmap[flow][0]
                continue

            max_level = 0
            for confidence_level in some_revmap[flow]:
                if confidence_level > max_level:
                    max_level = confidence_level
            some_revmap[flow] = max_level

        return some_revmap

    @staticmethod
    def unwind_rev_map(some_revmap):
        """
        :param some_revmap:
        :return:
        """
        ndict = {}

        for flow in some_revmap:
            confidence_level = some_revmap[flow]
            if confidence_level not in ndict:
                ndict[confidence_level] = []
            ndict[confidence_level].append(flow)

        return ndict

    def process(self):

        """
            {
                67: ['UC4_SC5_NETWORK_DRIVE'],
                50: ['UC4_SC5_NETWORK_DRIVE'],
                40: ['UC1_TOKEN_ISSUE_VDI_FROM_CLASS'],
                0: ['UC2_SC3_GOOD_WORK_INTERPRETED_AS']}
        """
        normalized_mapping_dict = self.normalize_confidence_level(self.summarized_mapping)
        """
            {
                67: ['UC4_SC5_NETWORK_DRIVE'],
                50: ['UC4_SC5_NETWORK_DRIVE'],
                40: ['UC1_TOKEN_ISSUE_VDI_FROM_CLASS']}
        """
        reversed_mapping_dict = self.create_rev_map(normalized_mapping_dict)
        """
            {
                'UC4_SC5_NETWORK_DRIVE': [50, 67],
                'UC1_TOKEN_ISSUE_VDI_FROM_CLASS': [40]}
        """
        reversed_mapping_dict = self.remove_dupe_flows_by_confidence(reversed_mapping_dict)
        """
            {
                'UC4_SC5_NETWORK_DRIVE': 67,
                'UC1_TOKEN_ISSUE_VDI_FROM_CLASS': 40}
        """
        normalized_mapping_dict = self.unwind_rev_map(reversed_mapping_dict)
        """
            {
                67: ['UC4_SC5_NETWORK_DRIVE'],
                40: ['UC1_TOKEN_ISSUE_VDI_FROM_CLASS']}
        """

        return normalized_mapping_dict
