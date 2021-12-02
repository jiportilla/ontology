#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import os
import pprint

from base import BaseObject
from base import FileIO
from datamongo import CountryCode
from taskmda.mda.dmo import GenericTemplateAccess


class GenerateCountryLookup(BaseObject):

    def __init__(self,
                 is_debug: bool = False):
        """
        Created:
            14-May-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/254
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug

    @staticmethod
    def _build_lookup() -> dict:
        records = CountryCode().all()

        def _country_code(fields: dict) -> str:
            for field in record["fields"]:
                if field["name"] == "country_code":
                    return field["value"]

        def _country_name(fields: dict) -> str:
            for field in record["fields"]:
                if field["name"] == "country_name":
                    return field["value"]

        d_lookup = {}
        for record in records:
            d_lookup[_country_code(record["fields"])] = _country_name(record["fields"])

        return d_lookup

    def process(self):
        from taskmda.mda.dto import KbNames
        from taskmda.mda.dto import KbPaths

        results = self._build_lookup()
        the_json_result = pprint.pformat(results, indent=4)
        the_json_result = "{0} = {{\n {1}".format(
            KbNames.country_codes(), the_json_result[1:])

        the_template_result = GenericTemplateAccess.process()
        the_template_result = the_template_result.replace(
            "CONTENT", the_json_result)

        path = os.path.join(os.environ["CODE_BASE"],
                            KbPaths.country_codes())
        FileIO.text_to_file(path, the_template_result)
