# !/usr/bin/env python
# -*- coding: UTF-8 -*-


import unittest

from nlutext import MultiTextParser

IS_DEBUG = True


class MultiTextParserTest(unittest.TestCase):

    def execute_parser(self,
                       input_text: str,
                       ontology_names: list):
        mtp = MultiTextParser(is_debug=IS_DEBUG, ontology_names=ontology_names)
        actual_result = mtp.process(original_ups=input_text)
        self.assertIsNotNone(actual_result)

    def test_01(self):
        self.execute_parser("the bonding electron and python and redhat",
                            ontology_names=['base', 'biotech'])


if __name__ == '__main__':
    unittest.main()
