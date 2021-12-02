# !/usr/bin/env python
# -*- coding: UTF-8 -*-

from datamongo import TextQueryGenerator


def config(div_field, key_field, input_text, case_sensitive, expected_query):
    tqg = TextQueryGenerator(div_field=div_field,
                             key_field=key_field,
                             is_debug=True)
    actual_query = tqg.process(input_text=input_text,
                               case_sensitive=case_sensitive)
    assert actual_query == expected_query


# test all params
def test_query_key_and_multiple_divs():
    expected_query = {'$and': [
        {'$text': {'$search': '/^help desk$/i'}},
        {'$or': [
            {'div_field': ''},
            {'div_field': 'cloud'}]},
        {'key_field': '123456789'}]}
    config(div_field=['', 'cloud'],
           key_field="123456789",
           input_text='help desk',
           case_sensitive=False,
           expected_query=expected_query)


# test multiple div fields, no key field
def test_query_nokey_and_multiple_divs():
    expected_query = {'$and': [
        {'$text': {'$search': '/^help desk$/i'}},
        {'$or': [
            {'div_field': ''},
            {'div_field': 'cloud'}]}]}
    config(div_field=['', 'cloud'],
           key_field=None,
           input_text='help desk',
           case_sensitive=False,
           expected_query=expected_query)


# test single div field, no key field
def test_query_nokey_single_div():
    expected_query = {'$and': [
        {'$text': {'$search': 'help desk'}},
        {'div_field': ''}]}
    config(div_field=[''],
           key_field=None,
           input_text='help desk',
           case_sensitive=True,
           expected_query=expected_query)


# test key field only, no div field
def test_query_key_nodiv():
    expected_query = {'$and': [
        {'$text': {'$search': 'help desk'}},
        {'key_field': '123456789'}]}
    config(div_field=[],
           key_field="123456789",
           input_text='help desk',
           case_sensitive=True,
           expected_query=expected_query)


# text only query
def test_query_text_only():
    expected_query = {'$text': {'$search': 'help desk'}}
    config(div_field=[],
           key_field=None,
           input_text='help desk',
           case_sensitive=True,
           expected_query=expected_query)
