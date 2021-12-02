#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import unittest

from datagit.graph.dmo import GraphTextSplitter


class FindRepoIDTest(unittest.TestCase):

    def execute_standard_split(self,
                               input_text: str,
                               expected_result: str):
        actual_result = GraphTextSplitter(is_debug=True,
                                          input_text=input_text).process()
        self.assertEquals(actual_result, expected_result)

    def execute_camelcase_split(self,
                                input_text: str,
                                expected_result: str):
        result = GraphTextSplitter.split_camel_case(threshold=3,
                                                    input_text=input_text)
        self.assertEquals(result, expected_result)

    def test_standard_split(self):
        self.execute_standard_split(input_text="Merge pull request #1398 from GTS-CDO/CT_1111 #1111 MDA Regeneration",
                                    expected_result="Merge pull request #1398\\nfrom GTS CDO/CT 1111 #1111\\nMDA Regeneration")

    def test_camelcase_split(self):
        self.execute_camelcase_split(input_text='FileImportNodeBuilder',
                                     expected_result='File\\nImport\\nNode\\nBuilder')
        self.execute_camelcase_split(input_text='ImportAPI',
                                     expected_result='Import\\nAPI')


if __name__ == '__main__':
    unittest.main()
