#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pprint

from base import BaseObject

# TODO: move to external py_dict
post_process_flow_dict = {
    "UC1_TECHNICAL_SUPPORT_CAP": [
        {"UC1_CITRIX_ISSUE_FROM_CLASS"},
        {"UC1_LOGON_ISSUE_CAP_FROM_CLASS"}
    ],

    "UC1_TECHNICAL_SUPPORT_VDI": [
        {"UC1_CITRIX_ISSUE_FROM_CLASS"},
        {"UC1_LOGON_ISSUE_VDI_FROM_CLASS"}
    ]
}


class ConfidenceNormalizerByDictionary(BaseObject):
    def __init__(self, some_summary):
        """
        Purpose:
            1.  dictionary-injected component where an arbitrary flow 
                can be given higher weight than other arbitrary flows
            2.  arbitrary because no taxonomy or other semantic relationship 
                needs to be explicitly defined for this component to function
                (having said that, it would be nice to know why a given flow is being given priority;
                a linkback to a github issue would be useful)
        
            Dictionary Format:
                key: [value1, value2, ... value_n]
                -   each keyed entry can have 2+ values in the associated list
                -   the keyed flow will always take priority over the value flows

            Sample:    
                given this flow summary:
                    80: [alpha, beta, gamma]
                
                given this dictionary input:
                    alpha: [beta, gamma]
                
                the new flow summary will be:
                    80: [alpha]
                    70: [beta, gamma]

        Updated:
            30-Mar-2017
            craig.trim@ibm.com
            *   renamed from 'ConfidenceLevelPostProcessor'
                (overly generic name that did not describe the component purpose)
        Updated:
            17-Jul-2019
            craig.trim@ibm.com
            *   migrated from abacus-att
                git@github.ibm.com:abacus-implementation/abacus-att.git

        :param some_summary: 
        """
        BaseObject.__init__(self, __name__)
        self.summary = some_summary

    def get_max_confidence(self):
        """
        :return: max confidence of the summary
        """
        max_confidence = 0

        for key in self.summary:
            if key > max_confidence:
                max_confidence = key

        return max_confidence

    def process(self):
        the_max_confidence = self.get_max_confidence()

        if the_max_confidence not in self.summary:
            raise ValueError("Max Confidence Not Found:\n\tconfidence = {0}\n\tsummary = {1}".format(
                the_max_confidence,
                pprint.pformat(self.summary, indent=4)))

        the_max_confidence_flows = self.summary[the_max_confidence]
        if len(the_max_confidence_flows) < 2:
            return self.summary

        for key in post_process_flow_dict:
            for flow_set in post_process_flow_dict[key]:

                flow_set.add(key)
                if flow_set != set(the_max_confidence_flows):
                    continue

                self.summary[the_max_confidence] = [key]

                the_lowered_confidence = the_max_confidence - 10
                if the_lowered_confidence not in self.summary:
                    self.summary[the_lowered_confidence] = []

                self.summary[the_lowered_confidence] = list(flow_set.difference({key}))

        return self.summary
