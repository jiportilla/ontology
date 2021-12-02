#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject


class ConfidenceNormalizerByAbsoluteFlow(BaseObject):
    def __init__(self, some_summary):
        """
        Purpose:
            deduct confidence for specific flows by name
            
        Example:
            55 [    UC4_SC5_NETWORK_DRIVE-MAPPING_ISSUE, 
                    UC4_SC5_PRINTER-PRINTER_MAPPING_FROM_CLASS, 
                    UNDERSPECIFIED_OTHER    ]
            
            in this case we want to deduct confidence in  
                    UNDERSPECIFIED_OTHER
                    
            so the output from this component would be
            55 [    UC4_SC5_NETWORK_DRIVE-MAPPING_ISSUE, 
                    UC4_SC5_PRINTER-PRINTER_MAPPING_FROM_CLASS  ]
            45 [    UNDERSPECIFIED_OTHER    ]
            
        Reference:
            1.  https://github.ibm.com/abacus-implementation/Abacus/issues/1294#issuecomment-1992747
            
        Created:
            30-Mar-2017
            craig.trim@ibm.com
        Updated:
            17-Jul-2019
            craig.trim@ibm.com
            *   migrated from abacus-att
                git@github.ibm.com:abacus-implementation/abacus-att.git
        :param some_summary:
        """
        BaseObject.__init__(self, __name__)
        self.summary = some_summary

    def get_candidate_flows(self, some_flow_name):

        for key in self.summary:
            flows = self.summary[key]
            if len(flows) < 2:
                continue
            if some_flow_name not in flows:
                continue

            yield key

    def deduct(self, some_key, some_flow_name):
        confidence = some_key - 10

        if confidence not in self.summary:
            self.summary[confidence] = []
        self.summary[confidence].append(some_flow_name)

        _values = set(self.summary[some_key]) - set(some_flow_name)
        self.summary[some_key] = _values

    def process(self):
        for key in self.get_candidate_flows("UNDERSPECIFIED_OTHER"):
            self.deduct(key, "UNDERSPECIFIED_OTHER")

        return self.summary
