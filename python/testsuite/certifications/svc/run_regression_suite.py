# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from tabulate import tabulate

from base import BaseObject


class RunRegressionSuite(BaseObject):

    def __init__(self,
                 regression: dict,
                 segment_by_vendor: bool,
                 vendor_name: str = None,
                 log_results: bool = False,
                 is_debug: bool = False):
        """
        Created:
            9-Aug-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/680
        Updated:
            13-Aug-2019
            craig.trim@ibm.com
            *   add 'vendor-name' as a parameter
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/729
        :param regression:
        :param segment_by_vendor:
        :param vendor_name:
            the vendor to perform the regression on
                e.g., vendor='SAP' will only run the regression on SAP Certifications
            None     run regression on all certifications
        :param log_results:
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._log_results = log_results
        self._segment_by_vendor = segment_by_vendor

        self._svcresult = self._process(regression=regression,
                                        vendor_name=vendor_name)

    def svcresult(self) -> dict:
        return self._svcresult

    def _process(self,
                 regression: dict,
                 vendor_name: str = None) -> dict:
        """
        Purpose:
            Execute a Regression Suite
        :param regression:
            the configuration for a regression suite is a dictionary with three entries:
                {   'name': 'self-reported certifications',
                    'file': 'regressions/certifications.csv',
                    'suite': 'self_certification' }
        :param vendor_name:
            the vendor to perform the regression on
                e.g., vendor='SAP' will only run the regression on SAP Certifications
            None     run regression on all certifications
        :return:
            a dictionary of output
            {   'results':      <the results DataFrame>,
                'analysis':     <the analysis DataFrame>   }

            the 'results' DataFrame contains something like this:
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

            the 'analysis' DataFrame contains something like this:
                +----+----------+-----------------------+---------------+---------+
                |    |   Failed | Result                |   SuccessRate |   Total |
                |----+----------+-----------------------+---------------+---------|
                |  0 |        0 | ABAP for SAP HANA 2.0 |           100 |       6 |
                +----+----------+-----------------------+---------------+---------+
        """
        from testsuite.certifications.dmo import RegressionInputTransformer
        from testsuite.certifications.dmo import RegressionAnalysisSummary
        from testsuite.certifications.svc import AnalyzeRegressionSuite
        from testsuite.certifications.svc import RegressionSuiteController

        controller = RegressionSuiteController()
        df_input = RegressionInputTransformer(
            regression['file'],
            vendor_name=vendor_name,
            is_debug=self._is_debug).dataframe()

        fn = getattr(controller, regression['suite'])
        df_results = fn(df_input)

        if self._log_results:
            self.logger.debug('\n'.join([
                f"Regression Suite Results (name={regression['suite']})",
                tabulate(df_results,
                         headers='keys',
                         tablefmt='psql')]))

        df_gold_analysis, df_standard_analysis = AnalyzeRegressionSuite(
            df_results,
            is_debug=self._is_debug).results()

        df_summary = RegressionAnalysisSummary(
            regression_suite_name=regression['suite'],
            df_gold_analysis=df_gold_analysis,
            segment_by_vendor=self._segment_by_vendor,
            df_standard_analysis=df_standard_analysis,
            is_debug=self._is_debug).summary()

        return {
            "results": df_results,
            "summary": df_summary,
            "gold": df_gold_analysis,
            "standard": df_standard_analysis}
