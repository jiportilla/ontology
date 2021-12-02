#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from pandas import DataFrame

from base import BaseObject
from base import DataTypeError


class GenerateMeansRecord(BaseObject):
    """
    Purpose:
        Given 1..* XDM (dimensionality) records, generate mean values across the slots
    Sample Input:
        Any list of dimensionality records from any collection
            (supply-xdm, demand-xdm, learning-xdm, etc)
    Sample Output:
        {   'cloud': 8.236,
            'data science': 6.863,
            'database': 10.894,
             'hard skill': 25.158,
             'other': 33.166,
             'project management': 20.706,
            'service management': 13.044,
            'soft skill': 27.92,
            'system administrator': 16.198  }
    """

    def __init__(self,
                 records: list or DataFrame,
                 is_debug: bool = False):
        """
        Created:
            26-Sept-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1010
        """
        BaseObject.__init__(self, __name__)

        self._is_debug = is_debug
        self._records = records

        self.logger.debug('\n'.join([
            "Instantiated GenerateMeansRecords",
            f"\tdebug={is_debug}",
            f"\trecords=({type(records)}, {len(records)})"]))

    def _by_list(self):
        from cendantdim.jrs.dmo import MeanGenerationByList
        return MeanGenerationByList(records=self._records,
                                    is_debug=self._is_debug).process()

    def _by_dataframe(self):
        from cendantdim.jrs.dmo import MeanGenerationByDataFrame
        return MeanGenerationByDataFrame(df=self._records,
                                         is_debug=self._is_debug).process()

    def process(self):

        if type(self._records) == list:
            return self._by_list()
        elif type(self._records) == DataFrame:
            return self._by_dataframe()

        raise DataTypeError(f"Records DataType Not Recognized ("
                            f"actual={type(self._records)}, "
                            f"expected=[list, DataFrame])")
