#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import os

from base import BaseObject
from base import FileIO
from base import MandatoryParamError


class BluepagesResultMapper(BaseObject):
    def __del__(self):
        self.timer.log()

    def __init__(self,
                 some_result: dict):
        """
        Created:
            28-Nov-2018
            craig.trim@ibm.com
        """
        BaseObject.__init__(self, __name__)
        if not some_result:
            raise MandatoryParamError("Result")

        self.result = some_result

        self.mapping = self._load_mapping()

    @staticmethod
    def _load_mapping():
        input_path = os.path.join(os.environ["CODE_BASE"],
                                  "workspace/databp/databp/neo/dmo/bluepages_field_mapping.yml")
        return FileIO.file_to_yaml(input_path)

    def _case(self, d_result: dict, mapping_results: list) -> dict:
        for k in mapping_results:
            value = self.result[k]

            if isinstance(value, str):
                if self.mapping[k]["case"].lower() == "lower":
                    value = value.lower()
                elif self.mapping[k]["case"].lower() == "upper":
                    value = value.upper()

            d_result[self.mapping[k]["label"]] = value

        return d_result

    def _normalize(self, d_result: dict, mapping_results: list) -> dict:
        for k in mapping_results:
            value = self.result[k]

            if "normalize" in self.mapping[k]:
                synonyms = self.mapping[k]["normalize"]
                for d_syn in synonyms:
                    for key in d_syn:
                        for alt in d_syn[key]:
                            if value.lower() == alt.lower():
                                value = key

            if isinstance(value, str):
                if self.mapping[k]["case"].lower() == "lower":
                    value = value.lower()
                elif self.mapping[k]["case"].lower() == "upper":
                    value = value.upper()

            d_result[self.mapping[k]["label"]] = value

        return d_result

    def _add_email_domain(self, d_result: dict) -> dict:
        email = self.result["INTERNET"]
        if email:
            email_domain = email.split("@")[-1].strip().lower()
            d_result["email_domain"] = email_domain

        return d_result

    def process(self) -> dict:
        """
        :return:
        """

        d_result = {}
        mapping_results = [x for x in self.mapping if x in self.result]

        if "INTERNET" in self.result:
            d_result = self._add_email_domain(d_result)

        d_result = self._case(d_result, mapping_results)
        d_result = self._normalize(d_result, mapping_results)

        return d_result
