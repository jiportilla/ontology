#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import unittest

IS_DEBUG = True


class TestCharacterLevelTokenizer(unittest.TestCase):

    def _execute(self,
                 input_text: str,
                 expected_output: dict) -> None:
        from nlusvc.coords.dmo import CharacterLevelTokenizer

        actual_output = CharacterLevelTokenizer(is_debug=IS_DEBUG,
                                                input_text=input_text).process()
        self.assertIsNotNone(actual_output)
        self.assertTrue(type(actual_output) == dict)
        self.assertEqual(actual_output, expected_output)

    def test_process(self):
        svcresult = {0: {'normalized': 'the', 'original': 'the '},
                     1: {'normalized': 'first', 'original': 'first '},
                     2: {'normalized': 'interleukin', 'original': 'interleukin ('},
                     3: {'normalized': 'IL', 'original': 'IL)-'},
                     4: {'normalized': '15', 'original': '15'}}

        self._execute(input_text='the first interleukin (IL)-15',
                      expected_output=svcresult)


if __name__ == '__main__':
    unittest.main()
