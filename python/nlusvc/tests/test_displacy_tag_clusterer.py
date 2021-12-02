#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import unittest
from typing import Optional

from datamongo import BaseMongoClient

IS_DEBUG = True


class TestDisplacyTagClusterer(unittest.TestCase):
    __mongo_client = BaseMongoClient()

    @staticmethod
    def _get_label(tag_name: str,
                   d_result: dict) -> Optional[str]:
        for k in d_result:
            for tag in d_result[k]:
                if tag == tag_name:
                    return k

    def _execute_by_name(self,
                         a_tag_tuple: tuple,
                         expected_label: str,
                         xdm_schema: str,
                         ontology_name: str) -> None:
        from nlusvc.displacy.dmo import DisplacyTagClusterer

        d_result = DisplacyTagClusterer(tags=[a_tag_tuple],
                                        xdm_schema=xdm_schema,
                                        ontology_name=ontology_name,
                                        mongo_client=self.__mongo_client,
                                        is_debug=IS_DEBUG).process()

        self.assertIsNotNone(d_result)
        self.assertEquals(type(d_result), dict)
        self.assertTrue(len(d_result))

        actual_label = self._get_label(d_result=d_result,
                                       tag_name=a_tag_tuple[0])
        self.assertEquals(actual_label, expected_label)

    def _execute_base(self,
                      a_tag_tuple: tuple,
                      expected_label: str) -> None:
        self._execute_by_name(xdm_schema='supply',
                              ontology_name='base',
                              a_tag_tuple=a_tag_tuple,
                              expected_label=expected_label)

    def _execute_biotech(self,
                         a_tag_tuple: tuple,
                         expected_label: str) -> None:
        self._execute_by_name(xdm_schema='biotech',
                              ontology_name='biotech',
                              a_tag_tuple=a_tag_tuple,
                              expected_label=expected_label)

    def test_biotech_01(self):
        self._execute_biotech(('cytotoxic activity', 100), 'activity')
        self._execute_biotech(('cell', 100), 'anatomy')
        self._execute_biotech(('blood cell', 100), 'anatomy')
        self._execute_biotech(('white blood cell', 100), 'anatomy')
        self._execute_biotech(('lymphocyte', 100), 'anatomy')
        self._execute_biotech(('t cell', 100), 'anatomy')
        self._execute_biotech(('memory t cell', 100), 'anatomy')

    def test_process(self):
        self.test_biotech_01()


if __name__ == '__main__':
    unittest.main()
