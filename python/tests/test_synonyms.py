# !/usr/bin/env python
# -*- coding: UTF-8 -*-

import pytest

from nlusvc import TextAPI

from regression_rows import RegressionRows

CSV_FILE = 'resources/testing/regressions/synonyms.csv' 
textApi = TextAPI()

def _test_normalization(expected_text, input_text):
    assert textApi.normalize(input_text) == expected_text

@pytest.mark.parametrize("index,group,expected_text,input_text", RegressionRows(CSV_FILE).gold)
def test_normalization_gold(index, group, expected_text, input_text):
    _test_normalization(expected_text, input_text)

@pytest.mark.parametrize("index,group,expected_text,input_text", RegressionRows(CSV_FILE).lv)
def test_normalization_langvar(index, group, expected_text, input_text):
    _test_normalization(expected_text, input_text)