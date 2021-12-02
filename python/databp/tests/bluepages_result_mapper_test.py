#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import unittest

if False:
    from databp.core.dmo import BluepagesResultMapper


    class CreateLanguageDictTest(unittest.TestCase):
        tests = [
            {
                "original": {"OFFICE": "altERNatE"},
                "expected": {'office': 'NA'}
            },
            {
                "original": {"OFFICE": "mobile"},
                "expected": {'office': 'MOBILE'}
            },
            {
                "original": {"OFFICE": "home"},
                "expected": {'office': 'MOBILE'}
            }
        ]

        def test_1(self):
            for test in self.tests:
                actual_result = BluepagesResultMapper(test["original"]).process()
                self.assertEquals(actual_result, test["expected"])


if __name__ == '__main__':
    unittest.main()
