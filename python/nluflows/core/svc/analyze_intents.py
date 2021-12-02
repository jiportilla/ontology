#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject


class AnalyzeIntents(BaseObject):

    def __init__(self,
                 intent_prediction: dict,
                 is_debug: bool = False):
        """
        Created:
            18-Jul-2019
            craig.trim@ibm.com
            *   refactored out of abacus-mapping-api
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/453
        :param intent_prediction:
            intent prediction output from a prior step
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)
        self.is_debug = is_debug
        self.intent_prediction = intent_prediction

    def max_flow(self) -> list:
        flows = self.intent_prediction['output']
        max_flow = max([int(x) for x in list(flows.keys())])

        return flows[str(max_flow)]

    def is_chit_chat(self) -> (bool, str):
        for flow in self.max_flow():
            if "CHITCHAT" in flow.upper():
                return True, flow
        return False, None

    def is_command(self):
        for flow in self.max_flow():
            if "COMMAND" in flow.upper():
                return True, flow
        return False, None
