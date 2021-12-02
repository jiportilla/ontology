# !/usr/bin/env python
# -*- coding: UTF-8 -*-


import re
import urllib.parse

from base import BaseObject
from base import MandatoryParamError


class TextPreprocessor(BaseObject):
    """ Text Preprocessing """

    _sub_url = re.compile(r'https?:\S+')
    _split_path_components = re.compile(r'[_-]')
    _punctuation_signs = [".", ",", "!", "?", ":"]

    _sub_qa = re.compile(re.escape('q&a'), re.IGNORECASE)
    _sub_net = re.compile(re.escape('.net'), re.IGNORECASE)

    __d_equal_replacements = {  # GIT-1373-16032810
        "bs": "bachelor_of_science",
        "ba": "bachelor_of_arts",
        "be": "bachelor_of_engineering",
        "ee": "electrical_engineering",
        "jd": "doctoral_degree",
        "me": "mechanical_engineering",
        "ma": "master_of_arts",
        "ms": "master_of_science"}

    __d_inline_replacements = {  # GIT-1373-16032810
        "´": "'",
        "q&a": "question_and_answer",
        "m&a": "merger_and_acquisition",
        ".net": "microsoft_dot_net",
        "c++": "c_plus_plus",
        "u.s.": "united states",
        "e.g.,": "example",
        "e.g.": "example"}

    def __init__(self,
                 input_text: str,
                 is_debug: bool = False):
        """
        Created:
            28-Oct-2019
            craig.trim@ibm.com
            *   History:
                -   The logic in this class started life as a simple function in TextParser
                -   I had a need to pre-process text prior to the full normalization pipeline
                -   I didn't have the time / cycles to modify the normalization pipeline
            *   Refactoring:
                -   It's probably best if this module is merged into 'text-normalizer'
                -   There is no conceptual difference between "Preprocessing" text and "Normalizing" text
            *   Reference:
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1206
        Updated:
            08-Nov-2019
            xavier.verges@es.ibm.com
            *   Remove URLs but keep isolated HTTP
            *   Comment on some failed performance improvement attempts
        Updated:
            11-Nov-2019
            xavier.verges@es.ibm.com
            *   Extract text from some URLs.
                https://github.ibm.com/GTS-CDO/unstructured-analytics/pull/1304#issue-3740574
        Updated:
            14-Nov-2019
            craig.trim@ibm.com
            *   text segmentation improvements
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1365#issue-10831576
        Updated:
            15-Nov-2019
            craig.trim@ibm.com
            *   clean up text replacements
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1373#issuecomment-16032810
        :param input_text:
            Any input text
        """
        BaseObject.__init__(self, __name__)
        if not input_text:
            raise MandatoryParamError("Input Text")

        self._input_text = input_text
        self._is_debug = is_debug

    def _extract_text_from_urls(self, normalized: str) -> str:
        for url in re.findall(self._sub_url, normalized):
            words_for_url = []
            components = urllib.parse.urlsplit(url)
            path_components = urllib.parse.unquote(components.path).split('/')[1:]
            if components.hostname in ['github.com', 'github.ibm.com']:
                at_most_first_two = path_components[0: min(2, len(path_components))]
                for path_component in at_most_first_two:
                    words_for_url += self._split_path_components.split(path_component)
            elif 'wikipedia.org/wiki' in url:
                if len(path_components) >= 2:
                    words_for_url = self._split_path_components.split(path_components[1])
            else:
                for path_component in path_components:
                    if path_component.count('-') >= 2:
                        words_for_url += self._split_path_components.split(path_component)

            normalized = normalized.replace(url, ' '.join(words_for_url))
        return normalized

    def process(self) -> str:
        """
        this input
            Microsoft PerformancePoint Server, Certified
        is normalized to
            Microsoft PerformancePoint Server Certified
        and this becomes
            microsoft performancepoint_server certified
        and this results in a tag
            Microsoft Certification
        because tagging works better when we can perform skip-gramming at level-1

        by paying attention to punctuation we get
            Microsoft PerformancePoint Server PUNCTUATIONSTOP Certified
        and this becomes
            microsoft performancepoint_server punctuationstop certified
        and this prevents the tag from being formed
        :return:
            a normalized UPS
        """
        normalized_ups = self._input_text

        for text in self.__d_equal_replacements:
            if text.lower().strip() == normalized_ups.lower().strip():
                normalized_ups = self.__d_equal_replacements[text]

        for text in self.__d_inline_replacements:
            if text.lower().strip() == normalized_ups.lower().strip():
                normalized_ups = self.__d_inline_replacements[text]

        if "http" in normalized_ups.lower():
            normalized_ups = self._extract_text_from_urls(normalized_ups)

        # Xavier considered using __sub_punctuation = re.compile(r'[\.\,\!\?\:]') instead
        # of this loop, but ipython %timeit proved it was slower
        for p in self._punctuation_signs:
            if p in normalized_ups:
                normalized_ups = normalized_ups.replace(p, " PUNCTUATIONSTOP ")

        while "PUNCTUATIONSTOP  PUNCTUATIONSTOP" in normalized_ups:  # GIT-1365-10831576
            normalized_ups = normalized_ups.replace("PUNCTUATIONSTOP  PUNCTUATIONSTOP", "PUNCTUATIONSTOP")

        # Same thing as above with __sub_multiple_whitespace = re.compile(r'\s+')
        while "  " in normalized_ups:
            normalized_ups = normalized_ups.replace("  ", " ")

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Text Preprocessing Completed",
                f"\t{normalized_ups}"]))

        return normalized_ups
