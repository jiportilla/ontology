#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import unittest

from datamongo import BaseMongoClient
from nlusvc import TextAPI

IS_DEBUG = True


class TestGenerateDisplacySpans(unittest.TestCase):

    def _execute(self,
                 input_text: str,
                 expected_entities: list) -> None:
        mongo_client = BaseMongoClient('wftag')
        text_api = TextAPI(is_debug=IS_DEBUG, ontology_name='biotech')

        for sent in text_api.sentencizer(input_text):
            svcresult = text_api.displacy_spans(
                input_text=sent,
                xdm_schema='biotech',
                ontology_names=['base'],
                mongo_client=mongo_client,
                use_schema_elements=True)

            actual_entities = sorted([entity['text'].lower() for entity in svcresult[0]['ents']])
            for expected_entity in expected_entities:
                self.assertTrue(expected_entity.lower() in actual_entities)

    def biotech_01(self):
        input_text = """But I have met members of its AI team and seen presentations that convince me it's a leader.""".strip()

        self._execute(input_text, ['ai', 'employee', 'presentation'])

    def test_process(self):
        self.biotech_01()


if __name__ == '__main__':
    unittest.main()
