#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import unittest

from datadict import FindDimensions

IS_DEBUG = True


class TestFindDimensions(unittest.TestCase):

    def _execute_by_name(self,
                         tag_name: str,
                         expected_result: str,
                         xdm_schema: str,
                         ontology_name: str):
        dim_finder = FindDimensions(schema=xdm_schema,
                                    ontology_name=ontology_name,
                                    is_debug=IS_DEBUG)
        results = dim_finder.find(input_text=tag_name)

        self.assertIsNotNone(results)
        self.assertTrue(len(results))

        actual_result = results[0]
        self.assertEquals(actual_result, expected_result)

    def _execute_biotech(self,
                         tag_name: str,
                         expected_result: str):
        self._execute_by_name(tag_name=tag_name,
                              expected_result=expected_result,
                              xdm_schema='biotech',
                              ontology_name='biotech')

    def test_biotech_01(self):
        self._execute_biotech('anatomy', 'anatomy')
        self._execute_biotech('cell', 'anatomy')
        self._execute_biotech('bacteria', 'anatomy')
        self._execute_biotech('blood cell', 'anatomy')
        self._execute_biotech('white blood cell', 'anatomy')
        self._execute_biotech('Lymphocyte', 'anatomy')
        self._execute_biotech('t cell', 'anatomy')

        # the actual ontology label
        self._execute_biotech('memory t cell', 'anatomy')

        # rdfs:seeAlso relationship to 'memory t cell'
        self._execute_biotech('memory cd8 t cell', 'anatomy')

        # I am suprised this works, but OK ...
        self._execute_biotech('memory cd8 t_cell', 'anatomy')

    def test_biotech_02(self):
        self._execute_biotech('Interleukin 15', 'molecule')

    def test_process(self):
        self.test_biotech_01()
        self.test_biotech_02()


if __name__ == '__main__':
    unittest.main()
