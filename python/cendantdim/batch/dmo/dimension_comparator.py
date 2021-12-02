#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import numpy as np
from scipy import stats

from base import BaseObject
from base import MandatoryParamError


class DimensionComparator(BaseObject):
    """
    """

    def __init__(self,
                 source_dimension: list,
                 target_dimensions: list,
                 is_debug=False):
        """
        Created:
            28-Mar-2019
            craig.trim@ibm.com
        :param source_dimension:
        :param target_dimensions:
        """
        BaseObject.__init__(self, __name__)
        if not source_dimension:
            raise MandatoryParamError("Source Dimension")
        if not target_dimensions:
            raise MandatoryParamError("Target Dimension")

        self.is_debug = is_debug
        self.source_dimension = source_dimension
        self.target_dimensions = target_dimensions

    def process(self) -> list:

        source = np.array(self.source_dimension)
        target = np.matrix(self.target_dimensions)

        def _matmul() -> list:
            _sums = []
            for i in range(0, len(target)):
                _sums.append(np.matmul(target[i], source))
            return _sums

        _mat_mul = _matmul()
        _z_score = list(stats.zscore(_mat_mul))

        _final_scores = []
        for x in _z_score:
            def _round():
                result = round(x[0][0], 0)
                if result == 0:
                    return 0
                return int(result)

            _final_scores.append(_round())

        if self.is_debug:
            self.logger.debug("\n".join([
                "Generated Values",
                "\tMat.Mul: len={}".format(_mat_mul),
                "\tZ-Score: {}".format(_z_score),
                "\tFinal: {}".format(_final_scores)
            ]))

        return _final_scores
