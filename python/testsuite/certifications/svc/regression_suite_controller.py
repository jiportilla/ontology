# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from pandas import DataFrame


class RegressionSuiteController(object):
    """
    Created:
        9-Aug-2019
        craig.trim@ibm.com
        *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/681
    """

    @staticmethod
    def self_certification(df: DataFrame) -> DataFrame:
        from testsuite.certifications.dmo import SelfCertificationRegression
        return SelfCertificationRegression(df_regression=df).process()

    @staticmethod
    def synonyms(df: DataFrame) -> DataFrame:
        from testsuite.certifications.dmo import SynonymsRegression
        return SynonymsRegression(df_regression=df).process()
