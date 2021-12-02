# !/usr/bin/env python
# -*- coding: UTF-8 -*-


import pandas as pd
from pandas import DataFrame
from tabulate import tabulate

from base import BaseObject


class RegressionResultAnalysis(BaseObject):
    """ Analyzes Regression Result DataFrames
        and provides Descriptive Statistics on results

        The incoming DataFrame 'df-result' looks something like this:
            +----+---------------------------------------------+-----------------------+---------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------------+--------+
            |    | ActualResults                               | ExpectedResult        | InputText                                                                             | NormalizedText                                                                                                     | Pass   |
            |----+---------------------------------------------+-----------------------+---------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------------+--------|
            |  0 | Certification, SAP Business One             | ABAP for SAP HANA 2.0 | sap hana abap 2.0 certification from sap                                              | sap_business_one abap 2 0 certification from sap_center                                                            | False  |
            |  1 | Certification, SAP Business One, Specialist | ABAP for SAP HANA 2.0 | SAP Certified Development Specialist - ABAP for SAP HANA 2.0                          | sap_center certification development specialist abap for sap_business_one 2 0                                      | False  |
            |  2 | Certification, SAP Business One, Specialist | ABAP for SAP HANA 2.0 | The "SAP Certified Development Specialist - ABAP for SAP HANA 2.0" certification exam | the sap_business_one certification development specialist abap for sap_business_one 2 0 professional_certification | False  |
            |  3 | Certification, SAP Business One             | ABAP for SAP HANA 2.0 | ABAP for SAP HANA cert                                                                | abap for sap_business_one certification                                                                            | False  |
            |  4 | SAP Business One                            | ABAP for SAP HANA 2.0 | ABAP for SAP HANA 2.0. Exam Code                                                      | abap for sap_business_one 2 0 exam code                                                                            | False  |
            |  5 | Certification, Online, SAP Business One     | ABAP for SAP HANA 2.0 | SAP ABAP for HANA Certification Online Practice                                       | sap_business_one abap for hana certification online practice                                                       | False  |
            |  6 | Certification, SAP Business One, Specialist | ABAP for SAP HANA 2.0 | Certified Development Specialist - ABAP for SAP HANA 2.0                              | certification development specialist abap for sap_business_one 2 0                                                 | False  |
            +----+---------------------------------------------+-----------------------+---------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------------+--------+

        This DataFrame contains the results of a regression suite run against a CSV.
        This component will analyze a given DataFrame and generate descriptive statistics
        into an analysis DataFrame
            +----+----------+-----------------------+---------------+---------+
            |    |   Failed | Result                |   SuccessRate |   Total |
            |----+----------+-----------------------+---------------+---------|
            |  0 |        0 | ABAP for SAP HANA 2.0 |           100 |       6 |
            +----+----------+-----------------------+---------------+---------+
    """

    def __init__(self,
                 df_result: DataFrame,
                 is_debug: bool = False):
        """
        Created:
            9-Aug-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/683
        :param df_result:
            the results of a regression analysis
            Example:
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._df_results = self._process(df_result)

    def results(self) -> DataFrame:
        return self._df_results

    @staticmethod
    def _accuracy(total_records: float,
                  total_failed_records: float) -> float:
        if total_failed_records == 0:
            return float(100.0)
        if total_failed_records == total_records:
            return float(0.0)

        f_total = float(total_records)
        f_fails = float(total_failed_records)

        x = float((f_total - f_fails) / f_total) * 100.0
        return round(x, ndigits=1)

    def _add_summary(self,
                     total_records: float,
                     total_failed_records: float) -> dict:
        summarized_accuracy = self._accuracy(total_records=total_records,
                                             total_failed_records=total_failed_records)
        return {
            "Result": "Summary",
            "Vendor": None,
            "Total": total_records,
            "Failed": total_failed_records,
            "SuccessRate": summarized_accuracy}

    def _process(self,
                 df_result: DataFrame) -> DataFrame:

        results = []
        all_total = 0
        all_failure = 0

        for key in df_result['ExpectedResult'].unique():
            df2 = df_result[df_result['ExpectedResult'] == key]
            vendor = list(df2['Vendor'].unique())[0]

            total_records = len(df2)
            all_total += total_records

            total_fails = len(df2[df2['Pass'] == False])
            all_failure += total_fails

            accuracy = self._accuracy(total_records=total_records,
                                      total_failed_records=total_fails)

            results.append({
                "Result": key,
                "Vendor": vendor,
                "Total": total_records,
                "Failed": total_fails,
                "SuccessRate": accuracy})

        results.append(self._add_summary(total_records=all_total,
                                         total_failed_records=all_failure))

        df_analysis = pd.DataFrame(results)

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Analysis Results",
                tabulate(df_analysis,
                         headers='keys',
                         tablefmt='psql')]))

        return df_analysis
