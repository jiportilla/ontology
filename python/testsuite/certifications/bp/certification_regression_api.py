# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from base import FileIO
from testsuite.certifications.svc import RunRegressionSuite


class CertificationRegressionAPI(BaseObject):
    """ Regression API for Certifications   """

    def __init__(self,
                 config: list,
                 regression_name: str,
                 segment_by_vendor: bool = True,
                 is_debug: bool = False,
                 log_results: bool = False):
        """
        Created:
            13-Aug-2019
            craig.trim@ibm.com
            *   refactored out of regression-orchestrator
        :param regression_name:
            a particular regression suite to run
        :param segment_by_vendor:
        :param is_debug:
        :param log_results:
        """
        BaseObject.__init__(self, __name__)

        self._config = config
        self._is_debug = is_debug
        self._log_results = log_results
        self._regression_name = regression_name
        self._segment_by_vendor = segment_by_vendor

    def _run_regression_suite(self,
                              regression: dict,
                              vendor_name: str = None) -> dict:
        return RunRegressionSuite(
            regression=regression,
            vendor_name=vendor_name,
            is_debug=self._is_debug,
            log_results=self._log_results,
            segment_by_vendor=self._segment_by_vendor).svcresult()

    def all(self) -> dict or ValueError:
        """
        :return:
            the results from a single regression suite
        """
        for regression in self._config:
            if regression['suite'].lower() == self._regression_name.lower():
                return self._run_regression_suite(regression)

        raise ValueError(f"Regression Suite Not Found "
                         f"(name={self._regression_name})")

    def by_vendor(self,
                  vendor_name: str) -> dict or ValueError:
        """
        :param vendor_name:
            a particular vendor within the regression suite to focus on
        :return:
            the results from a single regression suite
        """
        for regression in self._config:
            if regression['suite'].lower() == self._regression_name.lower():
                return self._run_regression_suite(regression=regression,
                                                  vendor_name=vendor_name)

        raise ValueError(f"Regression Suite Not Found "
                         f"(name={self._regression_name}, "
                         f"vendor={vendor_name})")

