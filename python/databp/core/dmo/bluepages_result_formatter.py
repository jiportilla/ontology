#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from base import BaseObject
from base import MandatoryParamError


class BluepagesResultFormatter(BaseObject):
    def __del__(self):
        self.timer.log()

    def __init__(self,
                 some_result: str):
        """
        Created:
            28-Nov-2018
            craig.trim@ibm.com
        """
        BaseObject.__init__(self, __name__)
        if not some_result:
            raise MandatoryParamError("Result")
        self.result = some_result

    def process(self,
                filter_empty=False) -> dict:
        """
        :param filter_empty:
        :return:
        """
        d_result = {}

        for line in self.result.split("\n"):
            tokens = line.split(":")

            key = tokens[0].upper().strip()

            if filter_empty:
                tokens = [x.strip() for x in tokens if x]
                if len(tokens) == 2:
                    d_result[key] = tokens[1].strip()
            elif len(tokens) == 2:
                d_result[key] = tokens[1].strip()

        if d_result["INTERNET"]:
            d_result["INTERNET"] = d_result["INTERNET"].lower()

        return d_result
