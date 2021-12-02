#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import hashlib


class BaseHash(object):
    """ standard hash function

    Created:
        12-Apr-2019
        craig.trim@ibm.com
        *   refactored out of 'ingest-transform-routines'
    """

    @classmethod
    def hash(cls,
             some_value: str) -> str:
        """
        Purpose:
        -   hash a serial using a standard formula
        Notes:
        -   Can’t use standard “hash()” routine in Python because it’s randomized (to prevent DDOS attacks)
            https://www.poftut.com/python-hash-strings-and-lists-to-md5-sha256-sha512-with-hashlib-module/
        :param some_value:
        :return:
            a hash value for a serial number
        """

        if type(some_value) != str:
            raise NotImplementedError("\n".join([
                "Unexpected Data Type",
                f"\tActual: {type(some_value)}",
                f"\tExpected: str",
                f"\tValue: {some_value}"]))

        some_value = str(some_value).upper().strip()
        md5 = hashlib.md5(some_value.encode()).hexdigest()

        return md5

    @classmethod
    def serial_number(cls,
                      some_value: str) -> str:
        return "SN_{}".format(cls.hash(some_value))

    @classmethod
    def badge(cls,
              some_value: str) -> str:
        return "BG_{}".format(cls.hash(some_value))
