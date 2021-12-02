#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import statistics

from base import BaseObject
from base import MandatoryParamError


class DimensionCalculator(BaseObject):
    """
    """

    def __init__(self,
                 d_slots: dict,
                 is_debug=False):
        """
        Created:
            28-Mar-2019
            craig.trim@ibm.com
            *   refactored out of 'transform-dim-map'
        Updated:
            16-Apr-2019
            craig.trim@ibm.com
            *   add '_normalized_zscores'
            *   refactor code
        """
        BaseObject.__init__(self, __name__)
        if not d_slots:
            raise MandatoryParamError("Slots")

        self.d_slots = d_slots
        self.is_debug = is_debug

    @staticmethod
    def _normalized_zscores(values: list,
                            mean: float,
                            standard_deviation: float):
        """
        Compute a normalized z-score

        A z-Score measures the amount of deviation from the mean
            https://en.wikipedia.org/wiki/Standard_score

        for example:
            cloud:          5   =>  0.172440375
            data science:   10  =>  1.322042873
            hard skill:     2   =>  -0.517321124
            soft skill:     0   =>  -0.977162123
            etc.

        We can't compute weights using negative values,
            and we don't want to zero-out negative values;
            this would lose valuable information

        So the minimum z-score is multiplied by -1
            and added back to all the values in the list

        for example:
            cloud:          0.172440375 + -0.97716212 = 1.15
            data science:   1.322042873 + -0.97716212 = 2.30
            hard skill:     -0.51732112 + -0.97716212 = 0.46
            soft skill:     -0.97716212 + -0.97716212 = 0.00
            etc.

        and this gives a "normalized z-score" to use in computation

        :param values:
            the integer values that exist per slot

            for example:
                cloud:          5
                data science:   10
                hard skill:     2
                soft skill:     0
                etc.

        :param mean:
            the mean (average) of all values

        :param standard_deviation:
            the standard deviation
            https://en.wikipedia.org/wiki/Standard_deviation

        :return:
            a list of normalized z-scores
        """

        # Step: Compute the zScores
        l_z_scores = []
        for i in range(0, len(values)):
            l_z_scores.append(float(values[i] - mean) / standard_deviation)

        # Step: Find the minimum zScore
        min_z_score = min(l_z_scores)
        if min_z_score < 0:

            # Step: Must be non-negative
            min_z_score *= -1

        # Step: Add the min-zScore to each zScore
        return [x + min_z_score for x in l_z_scores]

    def process(self) -> list:

        names = [x for x in self.d_slots]

        values = [self.d_slots[x]["value"]
                  for x in self.d_slots]

        total = sum(values)
        if total == 0:
            if self.is_debug:
                self.logger.debug("\n".join([
                    "No Tags Extracted",
                    "\tNames: {}".format(names),
                    "\tValues: {}".format(values)]))
            return [0.0 for _ in self.d_slots]

        # Step: Compute the Mean (single value)
        mean = statistics.mean(values)

        # Step: Compute the Std.Dev (single value)
        standard_deviation = statistics.stdev(values)

        # Step: Compute the z-Scores (one per slot)
        l_z_scores = self._normalized_zscores(values=values,
                                              mean=mean,
                                              standard_deviation=standard_deviation)

        # Step: Compute the Weighted Average
        l_weighted_average = []
        for i in range(0, len(values)):
            l_weighted_average.append(float(values[i] / total))

        l_final_weights = []
        for i in range(0, len(values)):
            def _result() -> float:
                x = float(l_weighted_average[i] * l_z_scores[i])
                if x <= 0:
                    if values[i] > 0:
                        return 0.005
                    return 0.0
                return x

            l_final_weights.append(_result())

        if self.is_debug:
            self.logger.debug("\n".join([
                "Generated Values",
                "\tColumns: {}".format(names),
                "\tValues: {}".format(values),
                "\tTotal: {}".format(total),
                "\tMean: {}".format(mean),
                "\tStd.Dev: {}".format(standard_deviation),
                "\tZ-Score: {}".format(l_z_scores),
                "\tWeighted Mean: {}".format(l_weighted_average),
                "\tFinal Score: {}".format(l_final_weights)]))

        return l_final_weights
