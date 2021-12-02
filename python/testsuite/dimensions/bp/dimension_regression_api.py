# !/usr/bin/env python
# -*- coding: UTF-8 -*-


import os

from base import BaseObject


class DimensionRegressionAPI(BaseObject):
    """ Regression API for Dimensionality   """

    def __init__(self,
                 config: list,
                 is_debug: bool = False,
                 log_results: bool = False):
        """
        Created:
            20-Aug-2019
            craig.trim@ibm.com
        :param config:
        :param is_debug:
        :param log_results:
        """
        BaseObject.__init__(self, __name__)

        self._config = config
        self._is_debug = is_debug
        self._log_results = log_results
        self._input_file = self._find_input_file()

    def _find_input_file(self) -> str:
        """
        :return:
            the input file for the regression suite
        """
        for regression in self._config:
            if regression['suite'] == 'dimensions':
                file_path = regression['file']
                if not os.path.exists(file_path):
                    raise FileNotFoundError(f"Dimemsions Input File Path Not Found: "
                                            f"{file_path}")

                return file_path

        raise ValueError("Dimemsions Input File Path Not Defined")

    def by_serial_number(self,
                         serial_number: str):
        """
        Purpose:
            Load the Regression Test into a DataFrame
        :param serial_number:
            Optionally filter the regression test by Serial Number
            None        load all regressions
        :return:

        """
        from testsuite.dimensions.svc import RunRegressionSuite
        svc = RunRegressionSuite(input_file=self._input_file,
                                 is_debug=self._is_debug)
        return svc.by_serial_answer(serial_number)
