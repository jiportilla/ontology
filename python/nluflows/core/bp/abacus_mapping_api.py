#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from datamongo import BaseMongoClient
from nluflows.core.svc import AnalyzeIntents
from nluflows.core.svc import PredictIntents


class AbacusMappingAPI(BaseObject):
    """
    Perform Service Catalog Mapping
    """

    def __init__(self,
                 base_mongo_client: BaseMongoClient = None,
                 is_debug: bool = False):
        """
        Updated:
            21-Jun-2017
            craig.trim@ibm.com
            *   injection of mapping table via a parameter
                in support of
                    <https://github.ibm.com/abacus-implementation/Abacus/issues/1624>
            *   removed debug parameter (wasn't being used)
        Updated:
            17-Jul-2019
            craig.trim@ibm.com
            *   migrated from abacus-att
                git@github.ibm.com:abacus-implementation/abacus-att.git
            *   default mapping table to 'dict::mapping-table'
        """
        BaseObject.__init__(self, __name__)
        self.is_debug = is_debug
        self.base_mongo_client = base_mongo_client

    def prediction(self,
                   min_confidence: int = 80,
                   preprocess_text: bool = True,
                   persist_result: bool = True,
                   trigger_ts: float = None) -> PredictIntents:
        """
        :param min_confidence:
            int         the minimum confidence threshold to deal with
        :param persist_result:
            if True     persist the result to MongoDB
        :param preprocess_text:
            Remove Slack directives from unstructured text
            Sample Input:
                'Hello <@ULLURKNFR>'
            Sample Output:
                'Hello'
        :param trigger_ts:
            float       (Optional) the timestamp of the event
                        that triggered this request
        :return:
        """
        return PredictIntents(trigger_ts=trigger_ts,
                              min_confidence=min_confidence,
                              preprocess_text=preprocess_text,
                              persist_result=persist_result,
                              base_mongo_client=self.base_mongo_client,
                              is_debug=self.is_debug)

    def analysis(self,
                 intent_prediction: dict) -> AnalyzeIntents:
        """
        :param intent_prediction:
            output from 'intent-prediction' API function
        :return:
        """
        return AnalyzeIntents(intent_prediction=intent_prediction,
                              is_debug=self.is_debug)


if __name__ == "__main__":
    text = 'hello <@ULLURKNFR>'

    api = AbacusMappingAPI(is_debug=True)
    svcresult = api.prediction(min_confidence=80,
                               preprocess_text=True).process(text)
    print(api.analysis(svcresult).is_chit_chat())
