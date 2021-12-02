#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pprint

from base import BaseObject
from base import MandatoryParamError


class SourceRecordMerger(BaseObject):
    """ """

    def __init__(self,
                 d_source_collections: dict,
                 field_traceability: bool = True,
                 is_debug: bool = False):
        """
        Created:
            28-Mar-2019
            craig.trim@ibm.com
            *   refactored out of 'assemble-manifest-data'
        Updated:
            12-Apr-2019
            craig.trim@ibm.com
            *   has to handle both ingest (raw) and parse (annotated) data now
        Updated:
            16-Apr-2019
            craig.trim@ibm.com
            *   field-level filtering and other refactoring
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/97
        Updated:
            1-Aug-2019
            craig.trim@ibm.com
            *   improved logging in pursuit of
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/563#issuecomment-13630539
        Updated:
            17-Oct-2019
            craig.trim@ibm.com
            *   modify collection provenance
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1142#issuecomment-15366627
        Updated:
            07-Jan-2020
            xavier.verges@es.ibm.com
            *   enable skipping tracebility of fields (required to assemble badges)
        """
        BaseObject.__init__(self, __name__)
        if not d_source_collections:
            raise MandatoryParamError("Source Collections")

        self._is_debug = is_debug
        self._source_collections = d_source_collections
        self._field_traceability = field_traceability

    @staticmethod
    def _field_by_value(a_record: dict,
                        field_name: str) -> str:
        """
            sample input:
            {   'fields': [
                    {   'name': 'serial_number',
                        'type': 'text',
                        'value': 'SN-f4d5439a06e1889f54fe398c68845ea1',
                        'agent': 'system' },
                    ...
                    {   'name': 'badge_name',
                        'type': 'badge',
                        'value': 'IBM Cloud Essentials',
                        'agent': 'system' }] }

            assuming
                'field_name = serial_number'

            sample output:
                SN-f4d5439a06e1889f54fe398c68845ea1

        :param a_record:
        :param field_name:
        :return:
            the value of the field
        """
        for field in a_record["fields"]:
            if field["name"] == field_name:

                _type = type(field["value"])
                if _type == str:
                    return field["value"]

                raise NotImplementedError(f"Unexpected Data Type: "
                                          f"actual={_type}, "
                                          f"expected=str")
        return "unreachable"

    def _fields(self,
                a_record: dict,
                collection_name: str,
                valid_fields: list) -> list:
        """
        Purpose:
            extract and return a list of (valid) fields from a record
        :param a_record:
            a record is a dictionary object that contains 0..* field dictionaries
        :param collection_name:
            the name of the mongo collection the record was sourced from
        :return:
            a list of (valid) fields extracted from the dictionary object
        """
        fields = a_record["fields"]

        def _is_valid(a_field: dict) -> bool:
            """
            Purpose:
                determine if a field is valid
            :param a_field:
                a given field for a given record
            :return:
                True    if the field is valid
            """

            def _is_zero_length_str() -> bool:
                return type(a_field["value"]) == str and not len(a_field["value"])

            def _invalid_field_name() -> bool:
                return a_field["name"] not in valid_fields

            if not a_field["value"]:
                return False
            elif _is_zero_length_str():
                return False
            elif _invalid_field_name():
                return False

            return True

        # Step: Filter out invalid fields
        fields = [field for field in fields
                  if _is_valid(field)]

        # Step: Add Provenance to each field
        tokens = collection_name.split('_')
        collection_type = '_'.join(tokens[:len(tokens) - 1])
        for field in fields:
            if not self._field_traceability:
                del field['field_id']
            else:
                tokens = collection_name.split('_')
                collection_type = '_'.join(tokens[:len(tokens) - 1])
                field["collection"] = {
                    "name": collection_name,
                    "type": collection_type}

        return fields

    @staticmethod
    def _manifest_field_names(some_source_manifest: dict) -> list:
        """
        Purpose:
            return a list of manifest field names

        Notes:
            the source manifest may look like this
                database: cendant
                collection: badges
                fields:
                    - name: email_address
                    - name: serial_number_lookup
                    - name: serial_number
                    - name: badge_name
                    - name: date_of_hire

            the "fields" section lists the field names to be used in the assembly process

            this component will return the names as a list:
                [   email_address,
                    serial_number_lookup,
                    serial_number,
                    badge_name,
                    date_of_hire ]

        :param some_source_manifest:
            the source manifest definition (YAML)
        :return:
            a list of manifest field names
        """
        fields = [x["name"] for x in some_source_manifest["fields"] if x]
        fields = [x.lower().strip() for x in fields if x]

        return sorted(set(fields))

    def _merge_collection(self,
                          manifest_source: dict,
                          source_collection: str,
                          manifest_field_names: list,
                          d_index_by_key: dict):
        """
        Purpose:
            Merge a source collection into the master index
        :param manifest_source:
        :param source_collection:
        :param manifest_field_names:
        :param d_index_by_key:
        :return:
        """

        self.logger.debug(f"manifest_source records size:  "
                          f" {manifest_source['records'].collection} ")

        self.logger.debug(f"manifest_source key_field size:  "
                          f" {len(manifest_source['key_field'])} ")

        self.logger.debug(f"Before call for loop")

        for record in manifest_source["records"]:
            #self.logger.debug ("Here")
            # Step: Extract the Key Field and add to Index
            key_field = self._field_by_value(record,
                                             manifest_source["key_field"])
            #self.logger.debug(f"KeyField:  "
            #                  f" {manifest_source['key_field']} ")
            if not key_field:
                raise ValueError(f"No Key Field Found: "
                                 f"{pprint.pformat(record, indent=4)}")

            if key_field not in d_index_by_key:
                d_index_by_key[key_field] = {
                    "fields": []}

            # Extract non-key Fields and add to Index
            fields = self._fields(a_record=record,
                                  collection_name=source_collection,
                                  valid_fields=manifest_field_names)
            d_index_by_key[key_field]["fields"] += fields
            d_index_by_key[key_field]["div_field"] = manifest_source["div_field"]

        self.logger.debug(f"After call for loop")

    def process(self) -> dict:
        d_index_by_key = {}

        for source_collection in self._source_collections:
            manifest_source = self._source_collections[source_collection]
            manifest_field_names = self._manifest_field_names(manifest_source)

            self.logger.debug(f"manifest_source size:  "
                              f" {len(manifest_source['records'])} ")
            self._merge_collection(manifest_source=manifest_source,
                                   source_collection=source_collection,
                                   manifest_field_names=manifest_field_names,
                                   d_index_by_key=d_index_by_key)

            total_fields = len(d_index_by_key)
            self.logger.debug(f"Extracted "
                              f"{total_fields} total fields "
                              f"from {source_collection}")

        grand_total = len(d_index_by_key)
        self.logger.debug(f"Loaded a grand total of "
                          f"{grand_total} fields "
                          f"across {len(self._source_collections)} source collections")

        ctr = 0
        d_normal = {}
        for key_field in d_index_by_key:

            # this only happens if there was an error in the ingest process
            total_fields = len(d_index_by_key[key_field]["fields"])
            if total_fields > 2000:
                # if N is sufficiently high, is the merge routing operating correctly?
                source_collections = ', '.join([x for x in self._source_collections])
                self.logger.warning(f"Unexpected Record: "
                                    f"Total Fields={total_fields}, "
                                    f"Key Field={key_field}, "
                                    f"Source Collections={source_collections}")

            d_normal[key_field] = {
                "div_field": d_index_by_key[key_field]["div_field"],
                "fields": d_index_by_key[key_field]["fields"]}

            if ctr % 1000 == 0:
                self.logger.debug(f"Merge Normalization Progress: "
                                  f"{ctr}-{grand_total}")
            ctr += 1

        total_keyfields = len(d_normal)
        self.logger.debug(f"Tag Extraction Complete "
                          f"(total-keyfields = {total_keyfields})")

        return d_normal
