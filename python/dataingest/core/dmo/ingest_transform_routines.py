#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import re
from datetime import datetime

import textacy
from bs4 import BeautifulSoup

from datadict import FindCountryCode
from datadict import FindGeo
from datadict import FindLearningPattern

_geo_finder = FindGeo()
_country_code_finder = FindCountryCode()
_learning_pattern = FindLearningPattern()


class IngestTransformRoutines(object):
    """ Transformation Routines """

    @staticmethod
    def job_role_id(some_value: str) -> str:
        """
            cleanse all the various permutations of Job Role ID
            Updated:
                abhbasu3@in.ibm.com
                Purpose: added bands 7a/7b and 6a/6b to be treated as 7 and 6
                <https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/652>
        :param some_value:
        :return:
            Job Role ID
        """
        some_value = some_value.replace("'", "")

        if some_value.endswith(".0"):
            some_value = some_value.split(".")[0].strip()

        if some_value.startswith("0") and len(some_value) == 6:
            return some_value[1:]

        return some_value

    @staticmethod
    def band_level(some_value: str):
        some_value = some_value.lower()
        if some_value == "d":
            return 11
        if some_value == "c":
            return 12
        if some_value == "b":
            return 13
        if some_value == "a":
            return 14
        if some_value == "aa":
            return 15
        if bool(re.search('6[a-zA-Z]', some_value)):
            return 6
        if bool(re.search('7[a-zA-Z]', some_value)):
            return 7
        try:
            return int(some_value)
        except ValueError:
            print("Invalid Band Level Transformation: {}".format(some_value))

        return None

    @staticmethod
    def country_code_lookup(some_value: str):
        return _country_code_finder.find_by_code(some_value)

    @staticmethod
    def country_to_region(some_value: str):
        return _geo_finder.find_region_by_country(some_value)

    @staticmethod
    def cleanse_client_name(some_value: str):

        _l_known_unknowns = [
            "blocked from view",
            "multicommercial",
            "multi commercial",
            "confidential",
            "multiple accounts",
            "legal & general",
            "multi - commercial",
            "nan"]

        _d_replace = {
            "a t & t": "at&t",
            "!bm": "ibm"}

        _d_contains = {
            "ibm": "ibm",
            "global delivery": "ibm",
            "confidential": "unknown",
            "so delivery": "ibm"}

        some_value = some_value.lower().strip()

        for value in _l_known_unknowns:
            if some_value == value:
                return "unknown"

        for key in _d_contains:
            if key in some_value:
                return _d_contains[key]

        for key in _d_replace:
            if key in some_value:
                some_value = some_value.replace(key, _d_replace[key])

        return some_value

    @staticmethod
    def tag_list(some_value: str):
        tokens = some_value.split(",")
        tokens = [x.lower().strip() for x in tokens if x and len(x)]

        if not tokens or not len(tokens):
            return None

        return tokens

    @staticmethod
    def badge_name(some_value: str) -> str:
        if "Level III" in some_value:
            return some_value.replace("Level III", "Level 3")
        elif "Level II" in some_value:
            return some_value.replace("Level II", "Level 2")
        elif "Level I" in some_value:
            return some_value.replace("Level I", "Level 1")
        return some_value

    @staticmethod
    def region_lookup(some_value: str) -> str:
        if "NAN" == some_value.upper():
            return "na"

        return some_value.lower()

    @staticmethod
    def city_normalizer(some_value: str) -> str:
        return _geo_finder.normalize_city(some_value, search_in_str=True)

    @staticmethod
    def populate_empty_month(some_value: str or None) -> int:

        def has_value() -> bool:
            if not some_value:
                return False
            if some_value.strip().lower() == "none":
                return False

            return True

        if has_value():
            return some_value

        return datetime.now().month

    @staticmethod
    def populate_empty_year(some_value: str or None) -> int:

        def has_value() -> bool:
            if not some_value:
                return False
            if some_value.strip().lower() == "none":
                return False

            return True

        if has_value():
            return some_value

        return datetime.now().year

    @staticmethod
    def normalize_month(some_value: str) -> int:
        try:
            return int(some_value)
        except ValueError:
            print(f"Invalid Month: {some_value}")
            return -1

    @staticmethod
    def normalize_year(some_value: str) -> int:
        try:
            return int(some_value)
        except ValueError:
            print(f"Invalid Year: {some_value}")
            return -1

    @staticmethod
    def geo_preprocessor(some_value: str) -> str:
        return textacy.preprocess_text(some_value,
                                       # fix_unicode=True,  # 20-May-2019 causes error
                                       lowercase=True,
                                       no_urls=True,
                                       no_emails=True,
                                       no_phone_numbers=True,
                                       no_numbers=True,
                                       no_currency_symbols=True,
                                       no_punct=True,
                                       no_contractions=True,
                                       no_accents=True)

    @staticmethod
    def textacy_preprocess(some_value: str) -> str:
        return textacy.preprocess_text(some_value,
                                       # fix_unicode=True,  # 20-May-2019 causes error
                                       lowercase=False,
                                       no_urls=False,
                                       no_emails=False,
                                       no_phone_numbers=False,
                                       no_numbers=False,
                                       no_currency_symbols=False,
                                       no_punct=True,
                                       no_contractions=False,
                                       no_accents=True)

    @staticmethod
    def remove_double_quotes(some_value: str) -> str:
        return some_value.replace('"', '')

    @staticmethod
    def to_boolean(some_value: str) -> bool:
        if type(some_value) == int:
            return some_value != 0
        if type(some_value) == str:
            return str(some_value).lower().strip().startswith("y")
        return bool(some_value)

    @staticmethod
    def to_bool(some_value: str) -> bool:
        return IngestTransformRoutines.to_boolean(some_value)

    @staticmethod
    def to_int(some_value: str) -> int or str:
        try:
            return int(some_value)
        except ValueError:
            return some_value

    @staticmethod
    def simple_date(some_value: str) -> str:
        """
        :param some_value:
            assume input: 2017-07-06 02:24:55
        :return:
            return output: 2017-07-06
        """
        if " " in some_value:
            return some_value.split(" ")[0].strip()
        return some_value

    @staticmethod
    def remove_html(some_value: str) -> str:
        """
        :param some_value:
            some value with HTML
        :return:
            stripped HTML
        """

        if "<" not in some_value:
            return some_value

        some_value = some_value.replace("<", " <")
        some_value = some_value.replace(">", "> ")
        return BeautifulSoup(some_value,
                             features="html.parser").get_text()

    @staticmethod
    def learning_noise(input_text: str,
                       is_debug: bool = False) -> str:
        """
        :param input_text:
        :param is_debug:
        :return:
        """
        normalized = _learning_pattern.remove_noise(input_text)
        if is_debug and normalized != input_text:
            print("\n".join([
                f"Identified Noise Pattern",
                f"\tOriginal Text: {input_text}",
                f"\tNormalized Text: {normalized}"]))

        return normalized

    @staticmethod
    def open_seats_status(some_value: str) -> str:
        """
        Purpose:
            transform a numeric value into an understandable text form
        Created:
            17-Apr-2019
            craig.trim@ibm.com
            *   reference:
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/50#issuecomment-10801932
        :param some_value:
        :return:
            a string value
        """
        some_value = int(some_value)
        if some_value == 1:
            return "open"
        elif some_value == 2:
            return "draft"
        elif some_value == 3:
            return "withdrawn"
        elif some_value == 4:
            return "closed"
