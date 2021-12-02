#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from typing import Optional, Union

from base.core.dmo import DataTypeError
from base.core.dmo import MandatoryParamError


class RecordStructure(object):
    """
    Created:
        29-Nov-2019
        craig.trim@ibm.com
        *   based on field-structure
    """

    @classmethod
    def grammmar_record(cls,
                        fields: list,
                        key_field: Union[str, int],
                        div_field: Optional[str]) -> dict:
        """
        Purpose:
            Generate a Record Structure for MongoDB records (for Formal Parse/grammar records)
        :param fields:
        :param key_field:
        :param div_field:
        :return:
        """

        if type(fields) != list:
            raise DataTypeError("Fields, list")

        if not key_field:
            raise MandatoryParamError("Key Field")

        if type(key_field) == int or type(key_field) == float:
            key_field = str(key_field)

        return {
            "fields": fields,
            "key_field": key_field,
            "div_field": div_field,
            "manifest": {}}

    @classmethod
    def github_record(cls,
                      issue_id: str,
                      fields: list,
                      repo_name: str,
                      manifest_name: str,
                      key_field: str or int,
                      key_field_parent: str or int,
                      div_field: str = None) -> dict:
        """
        Purpose:
            Generate a Record Structure for MongoDB records (for GitHub records)
        :param fields:
        :param key_field:
        :param key_field_parent:
        :param manifest_name:
        :param repo_name:
        :param div_field:
        :param issue_id:
        :return:
        """

        if type(fields) != list:
            raise DataTypeError("Fields, list")

        if not key_field:
            raise MandatoryParamError("Key Field")

        if type(key_field) == int or type(key_field) == float:
            key_field = str(key_field)

        if key_field_parent:
            if type(key_field_parent) == int or type(key_field_parent) == float:
                key_field_parent = str(key_field_parent)

        def _key_field_parent() -> Optional[str]:
            if not key_field_parent:
                return None
            elif key_field_parent != key_field:
                return key_field_parent
            return key_field

        return {
            "fields": fields,
            "key_field": key_field,
            "key_field_parent": _key_field_parent(),
            "div_field": div_field,
            "manifest": {
                "repository": repo_name,
                "issue_id": issue_id,
                "url": manifest_name}}
