#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import itertools
import pprint
import statistics

from base import BaseObject


class LongDistanceScoring(BaseObject):
    __score = None

    def __init__(self,
                 d_input: dict,
                 is_debug: bool = False):
        """
        Created:
            21-Aug-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/796
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        if self._is_debug:
            self.logger.debug('\n'.join([
                "Initialized LongDistanceScoring",
                pprint.pformat(d_input, indent=4)]))

        self._process(d_input)

    def score(self) -> float:
        return self.__score

    @staticmethod
    def _cartesian(d_input: dict):
        """
        Purpose:
            Generate a Cartesian Product of a Series of Lists
        Implementation:
            1.  Given a Series of Lists
                [ [44, 12, 8], [49], [47] ]
            2.  Generate a Cartesian Product
                [(44, 49, 47), (12, 49, 47), (8, 49, 47)]
        Reference:
            https://stackoverflow.com/questions/533905/get-the-cartesian-product-of-a-series-of-lists
        :param d_input:
            Sample Input:
                {   'development':  [44, 12, 8],
                    'rational':     [49],
                    'test':         [47] }
        :return:
            a Cartesian Product of the input values
        """
        cartesian = []

        values = list(d_input.values())
        for element in itertools.product(*values):
            cartesian.append(element)

        return cartesian

    def _process(self,
                 d_input: dict,
                 inflation: float = 3.0) -> None:
        """
        Purpose:
            Determine the Score of a Long Distance Match

        Notes:
            A series is a sequence of integers representing the position of a token in unstructured text

        Steps:
            1.  Find the mean of the series
            2.  Subtract the mean from each element position
            3.  Take a sum of the absolute values

        Example 1:
            Sample Series:  (8, 49, 47)
            Mean:           34.7
            Score:          sum([26.7, 14.3, 12.3])
                            100 - 53.3
                            46.7%
        Example 2:
            Sample Series:  (44, 49, 47)
            Mean:           46.7
            Score:          sum([2.7, 2.3, 0.3])
                            100 - 7.10
                            92.9%

        Example 1 represents a token match with only 47% confidence because
            all three tokens have a wide dispersion from one another.

        Example 2 represents a token match with 93% confidence because
            all three tokens are in proximity to each other.

        :param d_input:
            Sample Input:
                {   'development':  [44, 12, 8],
                    'rational':     [49],
                    'test':         [47] }
        :param inflation:
            fully heuristic multiplier designed to decrease confidence levels
        :return:
            the highest score
        """
        product = self._cartesian(d_input)

        if self._is_debug:
            self.logger.debug(f"Cartesian Product: {product}")

        scores = []
        for series in product:
            mean = statistics.mean(series)
            _sum = sum([abs(x - mean) for x in series])

            def confidence():
                _inflation_sum = _sum * inflation
                if 100 - _inflation_sum < 0:
                    return 0
                return round(100 - _inflation_sum)

            if self._is_debug:
                self.logger.debug(f"Intermediate Output "
                                  f"(mean={round(mean)}, "
                                  f"sum={round(_sum)}, "
                                  f"confidence={confidence()})")

            scores.append(confidence())

        if self._is_debug:
            self.logger.debug(f"High Score: {max(scores)}")
        self.__score = max(scores)
