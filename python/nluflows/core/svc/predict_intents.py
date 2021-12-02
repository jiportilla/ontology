#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pprint

from base import BaseObject
from datamongo import BaseMongoClient
from datamongo import SlackEventCollection
from nlusvc import TextAPI


class PredictIntents(BaseObject):
    _text_api = None

    def __init__(self,
                 min_confidence: int,
                 trigger_ts: float = None,
                 key_by_flows: bool = False,
                 preprocess_text: bool = True,
                 persist_result: bool = True,
                 base_mongo_client: BaseMongoClient = None,
                 is_debug: bool = False):
        """
        Created:
            18-Jul-2019
            craig.trim@ibm.com
            *   refactored out of abacus-mapping-api
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/453
        :param min_confidence:
            int         the minimum confidence threshold to deal with
        :param trigger_ts:
            float       (Optional) the timestamp of the event
                        that triggered this request
        :param persist_result:
            if True     persist the result to MongoDB
        :param preprocess_text:
            if True     perform preprocessing on the input text
        :param key_by_flows:
            if True         the service result will look like
                            {       FLOW_NAME_1: 80,
                                    FLOW_NAME_2: 80,
                                    FLOW_NAME_3: 75 }
            if False        the service result will look like
                            {       80: [ FLOW_NAME_1, FLOW_NAME_2 ]
                                    75: [ FLOW_NAME_3 }
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)
        self.is_debug = is_debug
        self.trigger_ts = trigger_ts
        self.key_by_flows = key_by_flows
        self.persist_result = persist_result
        self.min_confidence = min_confidence
        self.preprocess_text = preprocess_text
        self.mapping_event_collection = SlackEventCollection(base_mongo_client=base_mongo_client,
                                                             collection_name="event_mapping")

    def service_result(self,
                       svcresult: dict,
                       input_text_original: str,
                       input_text: str,
                       input_tags: list or None) -> dict:
        if not input_tags:
            input_tags = []

        svcresult = {
            "ts": BaseObject.generate_tts(),
            "trigger_ts": self.trigger_ts,
            "input": {
                "tags": input_tags,
                "text": {
                    "original": input_text_original,
                    "normalized": input_text}},
            "output": svcresult,
            "options": {
                "preprocess_text": self.preprocess_text,
                "min_confidence": self.min_confidence,
                "key_by_flows": self.key_by_flows}}

        return svcresult

    def _by_text(self,
                 input_text: str) -> dict:
        from nluflows.mapping.bp import ServiceCatalogMapper
        from nluflows.summarize.bp import ServiceCatalogSummarizer

        if not self._text_api:
            self._text_api = TextAPI()

        original_text = input_text

        # Step: Tag the Input Text
        if self.preprocess_text:
            input_text = self._text_api.preprocess(input_text,
                                                   lowercase=True,
                                                   no_urls=True,
                                                   no_emails=True,
                                                   no_phone_numbers=True,
                                                   no_numbers=True,
                                                   no_currency_symbols=True,
                                                   no_punct=True,
                                                   no_contractions=True,
                                                   no_accents=True,
                                                   remove_inside_brackets=True)

        def _intent_not_tagged():
            return self.service_result(svcresult=self._intent_not_tagged(),
                                       input_text=input_text,
                                       input_text_original=original_text,
                                       input_tags=None)

        def _intent_not_mapped(some_input_tags: list):
            return self.service_result(svcresult=self._intent_not_mapped(),
                                       input_text=input_text,
                                       input_text_original=original_text,
                                       input_tags=some_input_tags)

        def _intent_mapped(analysis: dict,
                           some_input_tags: list):
            return self.service_result(svcresult=analyses,
                                       input_text=input_text,
                                       input_text_original=original_text,
                                       input_tags=some_input_tags)

        # Step: Parse the Input Text
        df_results = self._text_api.parse(input_text)
        if df_results.empty:
            return _intent_not_tagged()

        input_tags = sorted(df_results.Tag.unique())

        # Step: Map Tags to Intents
        analyses = ServiceCatalogMapper(some_tags=input_tags,
                                        is_debug=self.is_debug).process()
        if not analyses or not len(analyses):
            return _intent_not_mapped(some_input_tags=input_tags)

        # Step: Summarize Mapping Results
        analyses = ServiceCatalogSummarizer(some_analyses=analyses,
                                            is_debug=self.is_debug).process()
        if not analyses or not len(analyses):
            return _intent_not_mapped(some_input_tags=input_tags)

        # Step: Normalize Results
        analyses = self._normalize(analyses)
        if not analyses or not len(analyses):
            return _intent_not_mapped(some_input_tags=input_tags)

        # Step: Return Final Results
        return _intent_mapped(analyses, input_tags)

    @staticmethod
    def _intent_not_tagged() -> dict:
        return {'100': "UNKNOWN_NOT_TAGGED"}

    @staticmethod
    def _intent_not_mapped() -> dict:
        return {'100': "UNKNOWN_NOT_MAPPED"}

    def _normalize(self,
                   analyses: dict) -> dict:
        d_normal = {}
        for k in analyses:
            if k >= self.min_confidence:
                d_normal[str(int(k))] = analyses[k]

        return d_normal

    def process(self,
                input_text: str) -> dict:
        """
        :param input_text:
        :return:
            {   'input': {   'tags': ['Hello'],
                    'text':
                    {   'normalized': 'hello',
                        'original': 'Hello <@ULLURKNFR>' }},
                'options': {
                    'key_by_flows': False,
                    'min_confidence': 80,
                    'preprocess_text': True },
                'output': {
                    '80': ['CHITCHAT_GREETING'] },
                'trigger_ts': '1563491078.019700',
                'ts': '1563491080.2059991'
            }
        """
        svcresult = self._by_text(input_text=input_text)

        if self.is_debug:
            self.logger.debug("\n".join([
                "Intent Prediction Completed",
                pprint.pformat(svcresult)]))

        if self.persist_result:
            self.mapping_event_collection.save(svcresult)

        return svcresult
