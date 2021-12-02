# !/usr/bin/env python
# -*- coding: UTF-8 -*-


import pandas as pd
from pandas import DataFrame

from base import BaseHash
from base import BaseObject


class GenerateCsvReport(BaseObject):
    """
    Purpose:
    Service that generates a CSV feedback from a Skills-based pandas DataFrame

    Traceability:
    https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/141

    Prereq:
    the API call to generate the pandas DataFrame
    """

    def __init__(self,
                 df: DataFrame,
                 hash_serial_number: bool = True,
                 is_debug: bool = True):
        """
        Created:
            24-Apr-2019
            craig.trim@ibm.com
        Updated:
            30-Jul-2019
            craig.trim@ibm.com
            *   change DataFrame column name 'Serial_Number' to 'SerialNumber' per
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/525#issuecomment-13568633
            *   change DataFrame column name 'Text' to 'Result' per
                https://github.ibm.com/-cdo/unstructured-analytics/issues/525#issuecomment-13575039
        Updated:
            8-Nov-2019
            craig.trim@ibm.com
            *   aligned with facade pattern
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1293
        :param df:
            the dataframe containing the tokens and their variations
        :param hash_serial_number:
            if True (default)       return an MD5 hash of the serial number
            if False                return the actual Serial Number
                                    note - individual Serial Numbers are exposed internally
                                    to all employees in Bluepages
        :param is_debug:
            if True     increase log output at DEBUG level
        """
        BaseObject.__init__(self, __name__)

        self.df = df
        self.is_debug = is_debug
        self._hash_serial_number = hash_serial_number

    def process(self) -> DataFrame:
        """
        :return:
            a list suitable for writing to file as CSV
        """

        results = []
        for serial_number in self.df['SerialNumber'].unique():

            def _keyfield() -> str:
                if self._hash_serial_number:
                    return BaseHash.serial_number(serial_number)
                return serial_number

            def _total_evidence() -> int:
                df_sub = self.df[self.df['SerialNumber'] == serial_number]
                return len(df_sub['Result'].unique())

            results.append({"SerialNumber": _keyfield(),
                            "TotalEvidence": _total_evidence()})

        self.logger.debug("\n".join([
            f"CSV Transformation Complete "
            f"(total-lines={len(results)})"]))

        return pd.DataFrame(results)
