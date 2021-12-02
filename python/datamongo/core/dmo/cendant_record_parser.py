#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pprint
from typing import Optional

from base import BaseObject


class CendantRecordParser(BaseObject):
    """ Parse a Cendant Record structure """

    def __init__(self,
                 is_debug: bool = False):
        """
        Created:
            31-Dec-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1681#issuecomment-16873873
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug

    def field_value_by_name(self,
                            a_record: dict,
                            a_field_name: str) -> Optional[str]:

        fields = [field for field in a_record["fields"]
                  if field["name"].lower() == a_field_name.lower()]

        if len(fields) > 1:
            self.logger.error('\n'.join([
                "Unexpected Field Configuration",
                f"\tField Name: {a_field_name}",
                pprint.pformat(a_record)]))
            raise NotImplementedError
        elif not len(fields):
            self.logger.warning('\n'.join([
                f"Field Name Not Found (name={a_field_name})",
                pprint.pformat(a_record)]))
            return None

        value = fields[0]['value']
        if value is not None and ',' in value:
            return value.split(',')[0].strip()

        return value

    def field_values_by_name(self,
                             a_record: dict,
                             a_field_name: str) -> Optional[list]:

        fields = [field for field in a_record["fields"]
                  if field["name"].lower() == a_field_name.lower()]
        if not len(fields):
            self.logger.warning('\n'.join([
                f"Field Name Not Found (name={a_field_name})",
                pprint.pformat(a_record)]))

        return [field['value'] for field in fields]

    def field_value_by_names(self,
                             a_record: dict,
                             some_field_names: list) -> Optional[str]:

        some_field_names = [field_name.lower().strip()
                            for field_name in some_field_names]
        fields = [field for field in a_record["fields"]
                  if field["name"].lower() in some_field_names]

        if not len(fields):
            self.logger.warning('\n'.join([
                f"Field Names Not Found (name={some_field_names})",
                pprint.pformat(a_record)]))

        return fields[0]['value']

    @staticmethod
    def has_field(a_record: dict,
                  a_field_name: str) -> bool:
        fields = [field for field in a_record["fields"]
                  if field["name"].lower() == a_field_name.lower()]
        return len(fields) > 0
