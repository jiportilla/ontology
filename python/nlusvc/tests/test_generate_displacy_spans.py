#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import unittest

from datamongo import BaseMongoClient


class TestGenerateDisplacySpans(unittest.TestCase):

    @staticmethod
    def _execute(input_text: str,
                 ontology_name: str) -> list:
        from nlusvc.displacy.svc import GenerateDisplacySpans

        return GenerateDisplacySpans(xdm_schema='supply',
                                     input_text=input_text,
                                     ontology_names=[ontology_name],
                                     mongo_client=BaseMongoClient(),
                                     is_debug=True).process(title="Result Title")

    def base_01(self):
        input_text = """A blockchain,[1][2][3] originally block chain,[4][5] 
        is a growing list of records, called blocks, 
        that are linked using cryptography.""".strip().replace('\n', ' ')

        results = self._execute(input_text, ontology_name='base')
        self.assertIsNotNone(results)
        self.assertEquals(type(results), list)
        self.assertTrue(len(results))

    def biotech_01(self):
        input_text = """and the cytotoxic activity of natural killer 
        (NK) cells and memory CD8+ T cells""".strip()

        results = self._execute(input_text, ontology_name='biotech')
        self.assertIsNotNone(results)
        self.assertEquals(type(results), list)
        self.assertTrue(len(results))

    def biotech_02(self):  # GIT-1803
        input_text = """But I have met members of its AI team and seen presentations that convince me it's a leader.""".strip()  # lots of new lines and tabs

        results = self._execute(input_text, ontology_name='base')
        self.assertIsNotNone(results)
        self.assertEquals(type(results), list)
        self.assertTrue(len(results))

    def test_process(self):
        # self.base_01()
        # self.biotech_01()
        self.biotech_02()


if __name__ == '__main__':
    unittest.main()
