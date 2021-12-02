# !/usr/bin/env python
# -*- coding: UTF-8 -*-


import pandas as pd
from pandas import DataFrame

from base import BaseObject
from datadict import FindCertifications


class PerformCertificationLabelling(BaseObject):
    """
    Purpose:
    Proper Label Certification Report

    e.g., transform 'itil certification' => 'ITIL Certification'

    Traceability:
    https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/981
    """

    def __init__(self,
                 dataframe: DataFrame,
                 is_debug: bool = False):
        """
        Created:
            23-Sept-2019
            craig.trim@ibm.com
       Purpose:
            Execute the Report to find "Self-Reported Certifications"
        :param dataframe:
            dataframe
        :param is_debug:
            if True     increase log output at DEBUG level
        """
        BaseObject.__init__(self, __name__)

        self._is_debug = is_debug
        self._dataframe = dataframe

        if self._is_debug:
            self.logger.debug(f"Initialize PerformCertificationLabelling")

    def process(self) -> DataFrame:
        print (self._dataframe.head(5))


        return self._dataframe
