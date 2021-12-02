# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from base import FileIO
from testsuite.certifications.bp import CertificationRegressionAPI
from testsuite.dimensions.bp import DimensionRegressionAPI


class RegressionAPI(BaseObject):
    __config_path = 'resources/testing/test_config.yml'

    def __init__(self,
                 is_debug: bool = False,
                 log_results: bool = False):
        """
        Created:
            13-Aug-2019
            craig.trim@ibm.com
            *   refactored out of regression-orchestrator
        Updated:
            20-Aug-2019
            craig.trim@ibm.com
            *   refactored into 'regression-certification-api'
        :param is_debug:
        :param log_results:
        """
        BaseObject.__init__(self, __name__)

        self._is_debug = is_debug
        self._log_results = log_results
        self._config = self._load_config()

    def _load_config(self) -> list:
        return FileIO.file_to_yaml_by_relative_path(self.__config_path)['regressions']

    def certifications(self,
                       segment_by_vendor: bool = True) -> CertificationRegressionAPI:
        """
        Purpose:
            Execute Regression test on Certifications
        :param segment_by_vendor:
            True    aggregate results by vendor
        :return:
            regression result
        """
        return CertificationRegressionAPI(
            config=self._config,
            regression_name='self_certification',
            segment_by_vendor=segment_by_vendor,
            is_debug=self._is_debug,
            log_results=self._log_results)

    def synonyms(self,
                 segment_by_synonym: bool = True) -> CertificationRegressionAPI:
        """
        Purpose:
            Execute Regression test on Synonyms
        :param segment_by_synonym:
            True    aggregate results by synonym class
        :return:
            regression result
        """
        return CertificationRegressionAPI(
            config=self._config,
            regression_name='synonyms',
            segment_by_vendor=segment_by_synonym,
            is_debug=self._is_debug,
            log_results=self._log_results)

    def dimensions(self) -> DimensionRegressionAPI:
        """
        Purpose:
            Execute Regression test on Dimensions
            None    run test on all records
        :return:
            regression result
        """
        return DimensionRegressionAPI(
            config=self._config,
            is_debug=self._is_debug,
            log_results=self._log_results)


if __name__ == "__main__":
    api = RegressionAPI(is_debug=True, log_results=True)
    api.dimensions().by_serial_number("TEST100")
    api.synonyms(segment_by_synonym=True).all()
    api.certifications(segment_by_vendor=False).by_vendor("Redhat Certified Specialist")
