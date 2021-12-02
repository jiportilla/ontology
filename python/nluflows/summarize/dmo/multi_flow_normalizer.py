#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject


class MultiFlowNormalizer(BaseObject):
    def __init__(self, some_summarized_mapping):
        """
        given sample input:
            {   '100.0': [   
                    # single entry
                    'UC1_FROZEN_ONERESET_CAP_FROM_CLASS,UC1_FROZEN_ONERESET_VDI_FROM_CLASS'
            ]}   
        return:
            {   '100.0': [
                    # multiple entry
                    'UC1_FROZEN_ONERESET_CAP_FROM_CLASS',
                    'UC1_FROZEN_ONERESET_VDI_FROM_CLASS'
            ]}   
        Updated:
            17-Jul-2019
            craig.trim@ibm.com
            *   migrated from abacus-att
                git@github.ibm.com:abacus-implementation/abacus-att.git
        :param some_summarized_mapping:
        """
        BaseObject.__init__(self, __name__)
        self.summarized_mapping = some_summarized_mapping

    def process(self):
        ndict = {}

        for key in self.summarized_mapping:
            if key not in ndict:
                ndict[key] = []

            for flow in self.summarized_mapping[key]:
                if "," not in flow:
                    ndict[key].append(flow)
                else:
                    for value in flow.split(","):
                        ndict[key].append(value.strip())

        return ndict
