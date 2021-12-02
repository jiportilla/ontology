# !/usr/bin/env python
# -*- coding: UTF-8 -*-


import os

from pandas import DataFrame

from base import BaseObject
from base import MandatoryParamError
from dataingest import ExcelReader


class RegressionInputTransformer(BaseObject):
    """ Transforms the incoming Excel Spreadsheet into a pandas DataFrame
    """

    __columns = {"Cnum": str,
                 "Schema": str,
                 "Collection": str,
                 "Field": str,
                 "Text": str,
                 "Tags": str,
                 "Outcome": str}

    def __init__(self,
                 input_file: str,
                 serial_number: str = None,
                 is_debug: bool = False):
        """
        Created:
            20-Aug-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/786#issuecomment-13991003
        """
        BaseObject.__init__(self, __name__)
        if not input_file:
            raise MandatoryParamError("Input File")

        self._is_debug = is_debug
        self._df = self._process(input_file=input_file,
                                 serial_number=serial_number)

        self.logger.debug('\n'.join([
            "Instantiated RegressionInputTransformer",
            f"\tParam (name=input-file, value={input_file})",
            f"\tParam (name=serial-number, value={serial_number})"]))

    def dataframe(self):
        return self._df

    def _process(self,
                 input_file: str,
                 serial_number: str) -> DataFrame:
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

        df = ExcelReader.read_excel(skiprows=0,
                                    column_names=self.__columns,
                                    some_input_path=path,
                                    some_sheet_name="Sheet1")

        if self._is_debug:
            self.logger.debug(f"Loaded Input File "
                              f"(total-lines={len(df)})")

        if serial_number and serial_number.upper() != "ALL":
            df = df[df['Cnum'] == serial_number.upper()]
            if self._is_debug:
                self.logger.debug(f"Filtered Input File "
                                  f"(total-lines={len(df)})")

        return df
