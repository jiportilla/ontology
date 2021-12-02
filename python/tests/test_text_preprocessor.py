# !/usr/bin/env python
# -*- coding: UTF-8 -*-

import pytest

from nlutext import TextPreprocessor

def words_from_urls_test_cases():
    test_cases = [
        (
            "https://stackoverflow.com/questions/4841782/python-constructor-and-default-value?noredirect=1&lq=1",
            'python constructor and default value'
        ),
        (
            "https://stackoverflow.com/questions/4841782/python-constructor-and-default-value?noredirect=1&lq=1"
            " and "
            "https://stackoverflow.com/questions/48756441/if-else-statements-in-python?noredirect=1&lq=1",
            'python constructor and default value and if else statements in python'
        ),
        (
            "http", "http"
        ),
        (
            "https", "https"
        ),
        (
            "https://twitter.com/home", ""
        ),
        (
            "https://github.ibm.com/-cdo/unstructured-analytics/issues",
            " cdo unstructured analytics"
        ),
        (
            "https://github.ibm.com/-cdo",
            " cdo"
        ),
        (
            "https://github.ibm.com/", ""
        ),
        (
            "https://gist.github.com/xverges/4a8e77985fd35cc7b8d8", ""
        ),
        (
            "https://github.com/apache/spark/pull/26462",
            "apache spark"
        ),
        (
            "https://mermaidjs.github.io/mermaid-live-editor",
            "mermaid live editor"
        ),
        (
            "https://github.com/php/pecl-database-ibm_db2",
            "php pecl database ibm db2"
        ),
        (
            "https://en.wikipedia.org/wiki/spark_(software)",
            "spark (software)"
        ),
        (
            "https://en.wikipedia.org/wiki/C%2B%2B",
            "C++"
        ),
        (
            "https://wikipedia.org/wiki", ""
        ),
    ]
    for index, test_case in enumerate(test_cases):
        yield index, test_case[0], test_case[1]


@pytest.mark.parametrize("index, input_text, normalized", words_from_urls_test_cases())
def test_extract_words_from_urls(index, input_text, normalized):
    extracted = TextPreprocessor(input_text).process()
    assert(extracted == normalized)
