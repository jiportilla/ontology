#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from base import MandatoryParamError


class BluepagesEndpointQueryAdapter(object):

    @staticmethod
    def validate_internet_address(some_internet_address: str) -> None:

        if not some_internet_address:
            raise MandatoryParamError("Internet Address")
        if not isinstance(some_internet_address, str):
            raise ValueError("Expected Datatype is String")
        if "@" not in some_internet_address:
            raise ValueError("Expected Format is: 'janedoe@ibm.com'")
