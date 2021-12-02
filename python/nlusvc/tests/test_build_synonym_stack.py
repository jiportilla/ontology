#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import unittest

IS_DEBUG = True


class TestBuildSynonymStack(unittest.TestCase):

    def _execute_by_name(self,
                         a_tag: str,
                         expected_size: int,
                         ontology_name: str) -> None:
        from nlusvc.coords.svc import BuildSynonymStack

        svc = BuildSynonymStack(is_debug=IS_DEBUG,
                                ontology_name=ontology_name)
        d_syns = svc.process(a_tag)

        self.assertIsNotNone(d_syns)
        self.assertEqual(type(d_syns), dict)
        self.assertTrue(len(d_syns))

        self.assertTrue('patterns' in d_syns)
        self.assertTrue(len(d_syns['patterns']))

        print('\n'.join([
            "Synonym Stack Test Execution Completed",
            f"\tTotal Variants: {len(d_syns['patterns'])}",
            f"\tExpected Size: {expected_size}"]))
        self.assertTrue(len(d_syns['patterns']) >= expected_size)

    def _execute_base(self,
                      a_tag: str,
                      expected_size: int) -> None:
        self._execute_by_name(ontology_name='base',
                              a_tag=a_tag,
                              expected_size=expected_size)

    def _execute_biotech(self,
                         a_tag: str,
                         expected_size: int) -> None:
        self._execute_by_name(ontology_name='biotech',
                              a_tag=a_tag,
                              expected_size=expected_size)

    def test_biotech_01(self):
        self._execute_biotech('cytotoxic activity', 2)
        self._execute_biotech('White Blood Cell', 5)

    def test_process(self):
        self.test_biotech_01()


if __name__ == '__main__':
    unittest.main()
