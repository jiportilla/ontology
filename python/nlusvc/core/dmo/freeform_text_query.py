#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from base import FileIO
from base import MandatoryParamError
from datamongo import CendantCollection


class FreeformTextQuery(BaseObject):
    """
    Given free-form query input return exact matches

    Sample Input:
    [   mainframe, aix, pseries, xseries    ]

    Sample Output:
        { 'job_role_id-40663': ['Mainframe Services',
                                'This specialty pseries skills required to architect ',
                                ...
                                'Server Management Mainframe Service Line.'],

          'job_role_id-40673': ['Mainframe Services',
                                'Mainframe MW and DB',
                                ...
                                 'and Midrange Systems (MVS, z/VM,z/VSE, z/OS'],

          'job_role_id-40686': ['Mainframe Services',
                                'Mainframe MW and DB',
                                ...
                                'Mainframe Platform']
          ...
        }
    """

    def __init__(self,
                 some_query_strings: list):
        """
        Created:
            22-Mar-2019
            craig.trim@ibm.com
        """
        BaseObject.__init__(self, __name__)
        if not some_query_strings:
            raise MandatoryParamError("Query Strings")

        self.d_terms = {}
        self.query_strings = some_query_strings
        self.config = FileIO.file_to_yaml_by_relative_path(
            "resources/config/freeform_text_query.yml")

    def _records(self,
                 collection_name: str,
                 database_name: str = "cendant") -> list:
        col = CendantCollection(some_db_name=database_name,
                                some_collection_name=collection_name)
        records = col.all()

        self.logger.debug(f"Retrieved Records "
                          f"(total={len(records)}, "
                          f"collection={collection_name})")

        return records

    @staticmethod
    def _key_field(fields: list,
                   keyfield_name: str) -> str:
        keyfield_value = [x["value"] for x in fields if x["name"] == keyfield_name][0]
        return "{}-{}".format(keyfield_name,
                              keyfield_value)

    def _is_valid(self,
                  value: str) -> bool:
        if not value:
            return False
        if not len(value.strip()):
            return False

        value = value.lower().strip()
        for variant in self.query_strings:
            if variant in value:
                return True

        return False

    def _update_terms(self,
                      some_key: str,
                      some_value: str) -> None:
        """ update dictionary with key:value input """
        if some_key not in self.d_terms:
            self.d_terms[some_key] = []
        self.d_terms[some_key].append(some_value)

    def _iterate_records(self,
                         records: list,
                         keyfield_name: str) -> None:
        """
        iterate through all the records and create a key:list dictionary
        -   the key is the 'keyfield-name' (e.g. serial-number for CV data)
        -   the values are 'discriminative terms' found in each record
        :param records:
        :param keyfield_name:
        """
        s_total_keyfields = set()

        for record in records:
            the_keyfield = self._key_field(record["fields"],
                                           keyfield_name)
            s_total_keyfields.add(the_keyfield)

            long_text_fields = [x for x in record["fields"] if x["type"] == "long-text"]
            long_text_fields = [x for x in long_text_fields if self._is_valid(x["value"])]
            [self._update_terms(the_keyfield, x["value"]) for x in long_text_fields]

    def process(self) -> dict:
        for collection in self.config["collection"]:
            records = self._records(collection["name"])

            self._iterate_records(records,
                                  collection["key"])

            self.logger.debug(f"Record Iteration Completed "
                              f"(terms={self.d_terms})")

        return self.d_terms
