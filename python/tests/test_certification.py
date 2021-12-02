# !/usr/bin/env python
# -*- coding: UTF-8 -*-

import pytest

from nlusvc import TextAPI

from regression_rows import RegressionRows

CSV_FILE = 'resources/testing/regressions/certifications.csv' 
textApi = TextAPI()

def _test_certification(expected_tag, input_text):

    results = textApi.parse(input_text)
    assert not results.empty
    assert expected_tag in set(results['Tag'].unique())

@pytest.mark.parametrize("index,group,expected_text,input_text", RegressionRows(CSV_FILE).gold)
def test_certification_gold(index, group, expected_text, input_text):
    _test_certification(expected_text, input_text)

@pytest.mark.parametrize("index,group,expected_text,input_text", RegressionRows(CSV_FILE).lv)
def test_normalization_langvar(index, group, expected_text, input_text):
    _test_certification(expected_text, input_text)