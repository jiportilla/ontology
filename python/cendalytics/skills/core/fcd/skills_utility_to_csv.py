# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from pandas import DataFrame

from base import BaseObject
from cendalytics.skills.core.svc import GenerateCsvReport


class SkillsUtilityToCSV(BaseObject):
    """ Facade: Service that generates a CSV feedback from a Skills-based pandas DataFrame """

    def __init__(self,
                 df_results: DataFrame,
                 hash_serial_number: bool = False,
                 is_debug: bool = False):
        """
        Created:
            8-Nov-2019
            craig.trim@ibm.com
            *   refactored out of skills-report-api
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1293
        :param df_results:
            a DataFrame of search results
        :param hash_serial_number:
            if True (default)       return an MD5 hash of the serial number
            if False                return the actual Serial Number
                                    note - individual Serial Numbers are exposed internally
                                    to all employees in Bluepages
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)

        self._is_debug = is_debug
        self._df_results = df_results
        self._hash_serial_number = hash_serial_number

    def _validator(self) -> None:
        from cendalytics.skills.core.dto import SkillsReportValidator
        if type(self._df_results) != DataFrame:
            SkillsReportValidator.expected(self._df_results, "pandas.DataFrame")
        if type(self._hash_serial_number) != bool:
            SkillsReportValidator.expected(self._hash_serial_number, "bool")

    def initialize(self) -> GenerateCsvReport:
        """
        :return:
            an instantiated service
        """

        # Step: Perform Data Validation
        self._validator()

        self.logger.info('\n'.join([
            "Instantiate CSV Report Generator",
            f"\tDF: {len(self._df_results)}",
            f"\tHash Serial Number? {self._hash_serial_number}"]))

        # Step: Instantiate Service
        return GenerateCsvReport(df=self._df_results,
                                 hash_serial_number=self._hash_serial_number)
