#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from base import BaseObject
from base import MandatoryParamError


class ExperienceYearsExtractor(BaseObject):
    """ extract the years of experience from a 'demand' text
        e.g. from open seats """

    def __init__(self,
                 some_input_text: str):
        """
        Created:
            13-Feb-2019
            craig.trim@ibm.com
        :param some_input_text:
            the CSV input file
        """
        BaseObject.__init__(self, __name__)
        if not some_input_text:
            raise MandatoryParamError("Input Text")

        self.input_text = some_input_text

    d_numbers = {
        "one": 1, "two": 2, "three": 3,
        "four": 4, "five": 5, "six": 6,
        "seven": 7, "eight": 8, "nine": 9, "ten": 10}

    @classmethod
    def _empty_result(cls) -> dict:
        return {}

    @classmethod
    def _result(cls,
                items: list) -> dict:
        """ return a result set """

        def _is_valid(a_year: int) -> bool:
            return 0 < a_year < 50

        def _min():
            a_min = 100
            for arg in items:
                if int(arg) < a_min:
                    a_min = int(arg)
            if _is_valid(a_min):
                return a_min

        def _max():
            a_max = 0
            for arg in items:
                if int(arg) > a_max:
                    a_max = int(arg)
            if _is_valid(a_max):
                return a_max

        the_min = _min()
        the_max = _max()

        if (the_min and the_min != 0) and not the_max:
            the_max = the_min
        elif not the_min and (the_max and the_max != 100):
            the_min = the_max
        elif not the_min and not the_max:
            return cls._empty_result()

        return {
            "min": the_min,
            "max": the_max
        }

    @classmethod
    def _normalize_text(cls,
                        text: str) -> str:
        """ normalize text to reduce patterns """
        text = text.lower().strip()

        text = text.replace("years", "year")
        text = text.replace("yrs ", "year ")
        text = text.replace("yr ", "year ")

        text = text.replace("at least", "minimum")

        text = text.replace("for over", "over")
        text = text.replace("+", " over ")
        text = text.replace("more than", "over")

        text = text.replace("-", " - ")
        text = text.replace("  ", " ").strip()

        return text

    @classmethod
    def _extract_phrase(cls,
                        tokens: list,
                        threshold: int) -> str:
        """ extract the portion of text that contains the 'years' description """
        for i in range(0, len(tokens)):
            if "year" not in tokens[i]:
                continue

            def _x():
                if i - threshold < 0:
                    return 0
                return i - threshold

            def _y():
                if i + threshold > len(tokens):
                    return len(tokens)
                return i + threshold

            phrase = " ".join(tokens[_x():_y()])
            return phrase.replace("\n", "").replace("\r", "").replace("\t", "").strip()

    @classmethod
    def _extract_years(cls,
                       phrase: str) -> list:
        """ extract int values as years """
        years = []
        for token in phrase.split(" "):
            if token.isdigit():
                try:
                    years.append(int(token))
                except ValueError:
                    continue
        return years

    @classmethod
    def extract(cls,
                text: str,
                threshold=5) -> dict:
        """
        :param text:
            the text to analyze
        :param threshold:
            this number represents a precision/recall tradeoff
            a higher value  casts a wider net and picks up more noise
            a lower value   is more precise but probably misses some patterns
        :return:
        """

        text = cls._normalize_text(text)
        if "yearly" in text:
            return cls._empty_result()

        if "year" in text or "yr":

            for k in cls.d_numbers:
                if k in text:
                    text = text.replace(k, str(cls.d_numbers[k]))

            phrase = cls._extract_phrase(text.split(" "),
                                         threshold)
            if not phrase or "year" not in phrase:
                return cls._empty_result()

            return cls._result(
                cls._extract_years(phrase))

    def process(self) -> dict:
        return self.extract(text=self.input_text)
