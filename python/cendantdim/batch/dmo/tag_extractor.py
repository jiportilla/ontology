#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from base import MandatoryParamError


class TagExtractor(BaseObject):
    """
    """

    def __init__(self,
                 some_manifest_source: dict,
                 some_records: list,
                 is_debug=False):
        """
        Created:
            26-Mar-2019
            craig.trim@ibm.com
        Updated:
            16-Apr-2019
            craig.trim@ibm.com
            *   modify computation to allow for 'system' tags
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/97
        Updated:
            15-May-2019
            craig.trim@ibm.com
            *   added band
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/243
        :param some_manifest_source:
        :param some_records:
        """
        BaseObject.__init__(self, __name__)
        if not some_manifest_source:
            raise MandatoryParamError("Manifest Source")
        if not some_records:
            raise MandatoryParamError("Records")

        self.is_debug = is_debug
        self.records = some_records
        self.d_manifest_source = some_manifest_source

    @staticmethod
    def _init_record() -> dict:
        """
            there are three types of tags:
            1.  User-supplied
                A.  Supervised Learning
                B.  Unsupervised Learning
            2.  System-supplied
                A.  Supervised Learning
                (Unsupervised doesn't exist yet - 16-Apr-2019)
        :return:
            empty data structure
        """
        return {"supervised": [],
                "unsupervised": [],
                "system": [],
                "attributes": {}}

    @staticmethod
    def _filter_fields(a_record: dict,
                       some_agent: str,
                       valid_field_types: list) -> list:

        # Function: Validate agent and field-type
        def _is_valid(a_field: dict) -> bool:
            if a_field["agent"].lower() != some_agent.lower():
                return False
            if "tags" not in a_field:
                return False
            if valid_field_types:
                if a_field["type"].lower() not in [x.lower().strip()
                                                   for x in valid_field_types]:
                    return False
            return True

        # Step: Filter out invalid fields
        return [x for x in a_record["fields"]
                if _is_valid(x)]

    @staticmethod
    def _extract_tags(fields: list,
                      tag_type: str) -> list:
        tags = set()
        for field in fields:
            if tag_type not in field["tags"]:
                continue

            field_tags = field["tags"][tag_type]
            if not field_tags or not len(field_tags):
                continue

            tags = tags.union(set(field_tags))

        return sorted(tags)

    def _supervised_user_tags(self,
                              a_record: dict) -> list:
        """
        :param a_record:
        :return:
            a list of all the tags for this record
                extracted via Supervised learning techniques
                from user-supplied text
            each tags also exists in the Cendant Ontology
        """
        user_fields = self._filter_fields(a_record=a_record,
                                          some_agent="user",
                                          valid_field_types=["long-text"])
        return self._extract_tags(user_fields,
                                  "supervised")

    def _unsupervised_user_tags(self,
                                a_record: dict):
        """
        :param a_record:
        :return:
            a list of all the tags for this record
                extracted via Unsupervised learning techniques
                from user-supplied text
        """
        user_fields = self._filter_fields(a_record=a_record,
                                          some_agent="user",
                                          valid_field_types=["long-text"])
        return self._extract_tags(user_fields,
                                  "unsupervised")

    def _supervised_system_tags(self,
                                a_record: dict) -> list:
        """
        :param a_record:
        :return:
            a list of all the tags for this record
                extracted via Supervised learning techniques
                from system-supplied text
            each tags also exists in the Cendant Ontology
        """
        user_fields = self._filter_fields(a_record=a_record,
                                          some_agent="system",
                                          valid_field_types=[])

        return self._extract_tags(user_fields,
                                  "supervised")

    def process(self) -> dict:
        d_records = {}
        for record in self.records:

            keyfield = record["key_field"]
            if keyfield not in d_records:
                d_records[keyfield] = self._init_record()

            d_records[keyfield]["system"] += self._supervised_system_tags(record)
            d_records[keyfield]["supervised"] += self._supervised_user_tags(record)
            d_records[keyfield]["unsupervised"] += self._unsupervised_user_tags(record)

            # ADDED on 15-May (Refactor into Method)
            def _field_by_name(a_field_name: str):
                for field in record["fields"]:
                    if field["name"] == a_field_name:
                        return field["value"]

            d_records[keyfield]["attributes"] = {
                "band": {"high": _field_by_name("high_band"),
                         "low": _field_by_name("low_band")}}
            # END refactor request

        if self.is_debug:
            self.logger.debug("\n".join([
                "Extracted Tags from Records",
                "\tTotal: {}".format(len(d_records))]))

        return d_records
