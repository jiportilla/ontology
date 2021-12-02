#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from pandas import DataFrame
from pandas import Series

from base import BaseObject
from base import FieldStructure
from base import MandatoryParamError


class IngestFieldMapping(BaseObject):
    """ map source fields to target fields based on the ingest manifest """

    def __init__(self,
                 some_manifest: dict,
                 some_source_df: DataFrame):
        """
        Created:
            11-Mar-2019
            craig.trim@ibm.com
        Updated:
            18-Aug-2019
            craig.trim@ibm.com
            *   added field-id
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/782
        Updated:
            20-Aug-2019
            craig.trim@ibm.com
            *   refactored field structure and field-id generate into field-structure
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/786
        Updated:
            22-Aug-2019
            craig.trim@ibm.com
            *   weed out fields with values of None
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/786#issuecomment-14042606
        :param some_manifest:
            the name of the ingestion activity
        :param some_source_df:
            the source dataframe
        """
        BaseObject.__init__(self, __name__)
        if not some_manifest:
            raise MandatoryParamError("Manifest")

        self.manifest = some_manifest
        self.source_df = some_source_df

    @staticmethod
    def _source_field(row: Series,
                      source_name):

        def _single_source_field(a_name: str) -> str:
            if source_name in row:
                return str(row[a_name])

            raise ValueError("\n".join([
                "Source Field Not Found",
                "\tName: {}".format(a_name)]))

        if type(source_name) == str:
            return _single_source_field(source_name)

        raise NotImplementedError("\n".join([
            "Unexpected Configuration",
            "\tSource Name: {}".format(source_name)]))

    def _field(self,
               field_name: str,
               field_value: str,
               field_type: str = "text",
               transformations=None) -> dict:

        def cleanse():
            if field_value:
                if field_type == "int":
                    return int(field_value.strip())
                ftype = type(field_type)
                if ftype == int or ftype == float:
                    return field_value
                return field_value.strip()

        _agent = self.manifest["source"]["agent"]
        field_value = cleanse()

        if field_value:
            return FieldStructure.generate_src_field(agent_name=_agent,
                                                     field_type=field_type,
                                                     field_name=field_name,
                                                     field_value=field_value,
                                                     transformations=transformations)

    def _handle_field(self,
                      row,
                      field: dict):

        source_value = self._source_field(
            row, field["source_name"])

        if field["target_name"] == "learning_id":
            if not source_value:
                raise ValueError(row)

        def _transformations():
            if "transformations" in field:
                return field["transformations"]

        return self._field(field_name=field["target_name"],
                           field_value=source_value,
                           field_type=field["data_type"],
                           transformations=_transformations())

    def process(self) -> list:

        target_records = []
        fields = self.manifest["fields"]
        total_source_records = len(self.source_df)

        def _target_records():

            ctr = 0
            for _, row in self.source_df.iterrows():
                ctr += 1

                target_fields = []
                for field in fields:
                    target_fields.append(self._handle_field(row, field))

                target_fields = [x for x in target_fields if x]

                target_records.append({
                    "fields": target_fields,
                    "manifest": {
                        "name": self.manifest["name"]}})

                if ctr % 10000 == 0:
                    self.logger.debug(f"Ingest Field Mapping: "
                                      f"{ctr} - {total_source_records}")

            return target_records

        target_records = _target_records()
        self.logger.debug("\n".join([
            "Mapped Source to Target Records",
            "\tTotal Source: {}".format(total_source_records),
            "\tTotal Target: {}".format(len(target_records))]))

        return target_records
