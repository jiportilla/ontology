# !/usr/bin/env python
# -*- coding: UTF-8 -*-

import pytest

from testsuite.certifications.dmo import RegressionInputTransformer

class RegressionRows(object):

    __cache = {}

    def __init__(self, full_path):
        if not full_path in self.__cache:
            self.__cache[full_path] = {
                'gold': [],
                'lv': []
            }
            for_this_file = self.__cache[full_path] 
            frame = RegressionInputTransformer(full_path, is_debug=True).dataframe()
            for index, row in frame.iterrows():
                group = row["Vendor"]
                expected = row["ExpectedResult"] 
                input_text = row["Input"]
                param = pytest.param(index,
                                    group,
                                    expected,
                                    input_text)
                                    #marks=[pytest.mark.regression,
                                    #       pytest.mark.slowish]))
                if group == 'gold' or expected == input_text:
                    for_this_file['gold'].append(param)
                else:
                    for_this_file['lv'].append(param)
        self.gold = self.__cache[full_path]['gold']
        self.lv = self.__cache[full_path]['lv'] 