#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import unittest


class TestTextStringMatcher(unittest.TestCase):

    @staticmethod
    def _execute(match_token: str,
                 input_text: str) -> bool:
        from nlusvc.core.svc import TextStringMatcher

        return TextStringMatcher(is_debug=True,
                                 a_token=match_token,
                                 some_text=input_text).process()

    def test_process(self):
        match_token = "bright field microscopy"

        input_text = """Abstract: The current study detects different morphologies related to 
        prostate pathology using deep learning models; these models were evaluated on 2,121 
        hematoxylin and eosin (H&E) stain histology images captured using bright field microscopy,,,,which 
        spanned a variety of image qualities, origins (whole slide, tissue micro array, whole mount, Internet), 
        scanning machines, timestamps,  H&E staining protocols, and institutions.""".strip()

        self.assertTrue(self._execute(match_token, input_text))


if __name__ == '__main__':
    unittest.main()
