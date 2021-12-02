# !/usr/bin/env python
# -*- coding: UTF-8 -*-

"""
Created: 2019-05-05
Owner: anassar@us.ibm.com
"""


class BusinessException(Exception):
    def __init__(self, business_unit="unknown", message="unknown"):
        self.message = message
        self.business_unit = business_unit
