# !/usr/bin/env python
# -*- coding: UTF-8 -*-


import os

import pandas as pd
from pandas import DataFrame

from base import BaseObject
from base import MandatoryParamError


class RegressionInputTransformer(BaseObject):
    """ Transforms the incoming CSV to DF
    """

    __columns = {'Vendor': str,
                 'ExpectedResult': str,
                 'Input': str}

    __na_values = ['none', 'None', 'na', '(None)']

    def __init__(self,
                 input_file: str,
                 vendor_name: str = None,
                 is_debug: bool = False):
        """
        Created:
            9-Aug-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/681
        Updated:
            12-Aug-2019
            craig.trim@ibm.com
            *   add 'vendor' column
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/717
            *   use 'pd.read-csv' function
        Updated:
            13-Aug-2019
            craig.trim@ibm.com
            *   perform additional filtering by vendor
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/729
        Updated:
            14-Aug-2019
            craig.trim@ibm.com
            *   user can filter by expected result (using vendor param)
            *   don't force lower-case back into the dataframe
        """
        BaseObject.__init__(self, __name__)
        if not input_file:
            raise MandatoryParamError("Input File")

        self._is_debug = is_debug
        self._df = self._process(input_file=input_file,
                                 vendor_name=vendor_name)

    def dataframe(self):
        return self._df

    def _process(self,
                 input_file: str,
                 vendor_name: str = None) -> DataFrame:
        """
        Purpose:
            Transform the CSV file to a DataFrame
        Sample Input:
            SAP, ABAP for SAP HANA 2.0,sap hana abap 2.0 certification from sap
            SAP, ABAP for SAP HANA 2.0,SAP Certified Development Specialist - ABAP for SAP HANA 2.0
            SAP, ABAP for SAP HANA 2.0,ABAP for SAP HANA cert
        Sample Output:
            +----+--------+-----------------------+---------------------------------------------------------------+
            |    | Vendor | ExpectedResult        | Input                                                         |
            |----+--------+-----------------------+---------------------------------------------------------------|
            |  0 | SAP    | ABAP for SAP HANA 2.0 | sap hana abap 2.0 certification from sap                      |
            |  1 | SAP    | ABAP for SAP HANA 2.0 | SAP Certified Development Specialist - ABAP for SAP HANA 2.0  |
            |  3 | SAP    | ABAP for SAP HANA 2.0 | ABAP for SAP HANA cert                                        |
            +----+--------------------------------+---------------------------------------------------------------+
        :return:
            a DataFrame representation of the CSV File
        """
        path = os.path.join(os.environ['CODE_BASE'], input_file)

        df = pd.read_csv(path,
                         engine="python",
                         sep=',',
                         skiprows=1,
                         comment='#',
                         encoding='utf-8',
                         dtype=self.__columns,
                         names=list(self.__columns.keys()),
                         na_values=self.__na_values,
                         parse_dates=True,
                         warn_bad_lines=True,
                         error_bad_lines=True,
                         skip_blank_lines=True,
                         skipinitialspace=True,
                         delim_whitespace=False,
                         infer_datetime_format=True)

        total_records = len(df)

        # Step: Filter by Vendor; return partial DataFrame
        if vendor_name:
            df_sub = df[df['Vendor'].str.lower() == vendor_name.lower()]

            # User might be searching by Expected Result
            if df_sub.empty:
                df_sub = df[df['ExpectedResult'].str.lower() == vendor_name.lower()]

            if self._is_debug:
                filtered_records = len(df_sub)
                self.logger.debug('\n'.join([
                    "DataFrame Loaded (Vendor Filter)",
                    f"\tTotal Records: {total_records}",
                    f"\tFiltered Records: {filtered_records}",
                    f"\tVendor: {vendor_name}"]))

            return df_sub

        # Step: No Filter Applied; return entire DataFrame
        if self._is_debug:
            self.logger.debug('\n'.join([
                "DataFrame Loaded (No Filter)",
                f"\tTotal Records: {total_records}"]))

        return df
