#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from typing import Union

import pandas as pd
from pandas import DataFrame
from tabulate import tabulate

from base import BaseObject
from base import DataTypeError
from base import EvidenceStructure
from base import MandatoryParamError
from datadict import FindDimensions


class EvidenceExtractor(BaseObject):
    """ Generates a Dataframe of 'Evidence' for a given Serial Number (or other Key Field)

        Given this input
            key-field =     1812546302
            schema-name =   dim

        Generate this Dataframe
            +-----+----------------------------------+--------------------+--------------------+-------------+-----------------------+--------------+
            |     | Collection      | FieldName      | NormalizedText     | OriginalText       | Schema      | Tag                   | TagType      |
            +-----+----------------------------------+--------------------+--------------------+-------------+-----------------------+--------------+
            |   0 | ingest_badges   | badge_name     | Mainframe Operator | Mainframe Operator | hard skill  | Communications Server | supervised   |
            +-----+----------------------------------+--------------------+--------------------+-------------+-----------------------+--------------+

        Note that actual Dataframe will may be large and span hundreds of rows,
        with column lengths for unstructed text fields (such as OriginalText and NormalizedText)
        being in excess of several hundred characters
    """

    __valid_tag_dtypes = [tuple, list]

    def __init__(self,
                 xdm_schema: str,
                 some_records: Union[list, dict],
                 is_debug: bool = False):
        """
        Created:
            3-May-2019
            craig.trim@ibm.com
            *   refactored out of tag-extractor with Jupyter notebooks in mind
        Updated:
            25-Aug-2019
            craig.trim@ibm.com
            *   incorporate use of tag tuples
                https://github.ibm.com/-cdo/unstructured-analytics/issues/818
        Updated:
            15-Oct-2019
            craig.trim@ibm.com
            *   add work-around for tag iteration defect
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1124#issue-10473464
        Updated:
            17-Oct-2019
            craig.trim@ibm.com
            *   refactor component in pursuit of
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1142#issuecomment-15370141
        Updated:
            25-Oct-2019
            craig.trim@ibm.com
            *   minor defect fix
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1199#issue-10601102
        Updated:
            29-Oct-2019
            craig.trim@ibm.com
            *   remove 'entity-schema-finder' in favor of new approach
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/916#issuecomment-15620801
            *   remove learning-pattern class (hasn't been used for a while)
        Updated:
            14-Nov-2019
            craig.trim@ibm.com
            *   add key-field to dataframe result
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1359#issue-10828085
            *   add transform-records
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1359#issuecomment-16009176
        :param some_records:
        :param xdm_schema:
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)
        if not some_records:
            raise MandatoryParamError("Records")

        self._is_debug = is_debug
        self._records = self._transform_records(some_records)
        self._xdm_finder = FindDimensions(schema=xdm_schema)

    @staticmethod
    def _transform_records(some_records: Union[dict, list]) -> list:
        """
        Purpose:
        -   Evidence Extraction is called by multiple APIs, and it's challenging to control
            the data types across Cendant
        -   Input to this class may be a
                dictionary (single record)
                list of dictionaries (multiple records)
                list of lists
                    where an inner list is composed of a list of records or
                    where an inner list is composed of a single record
        -   This function will transform all these various inputs into a flat list
        :param some_records:
            a variety of possibilities as noted above
        :return:
            a flat list
        """
        if type(some_records) == dict:
            return [some_records]
        elif type(some_records) == list:
            master = []
            for record in sorted(some_records):
                if type(record) == list:
                    master += sorted(record)
                elif type(record) == dict:
                    master.append(record)
                else:
                    raise DataTypeError
            return master
        raise DataTypeError

    def _schema(self,
                some_input: str) -> list:
        if not some_input:
            return []
        return self._xdm_finder.find(some_input)

    @staticmethod
    def _field_value(a_field: dict,
                     a_field_name: str) -> str:
        if a_field_name in a_field:
            if type(a_field[a_field_name]) == list and len(a_field[a_field_name]) > 0:
                return a_field[a_field_name][0]
            return a_field[a_field_name]

    @staticmethod
    def _supervised_tags(a_field: dict) -> list:
        if "supervised" in a_field["tags"] and a_field["tags"]["supervised"]:
            return [x for x in a_field["tags"]["supervised"] if x]
        return []

    @staticmethod
    def _unsupervised_tags(a_field: dict) -> list:
        if "unsupervised" in a_field["tags"]:
            return [x for x in a_field["tags"]["unsupervised"] if x]
        return []

    def _build_result(self,
                      field: dict,
                      key_field: str,
                      a_tag_name: str or None,
                      a_tag_score: float or None,
                      a_tag_type: str or None) -> dict:
        original_text = self._field_value(field, "value")

        def _normalized() -> str:
            if "normalized" in field:
                return self._field_value(field, "normalized")
            return original_text

        normalized_text = _normalized()

        def _collection_name():  # 1199#issue-10601102
            if type(field["collection"]) == dict:
                return field["collection"]["type"]
            return field["collection"]

        for schema_element in self._schema(a_tag_name):
            return EvidenceStructure.generate(
                key_field=key_field,
                collection_name=_collection_name(),
                field_name=field["name"],
                normalized_text=normalized_text,
                original_text=original_text,
                tag_name=a_tag_name,
                tag_score=a_tag_score,
                tag_type=a_tag_type,
                schema_name=schema_element)

    def _validate(self,
                  a_tag_tuple) -> bool:
        """
        :param a_tag_tuple:
            a candidate tag tuple
        :return:
            False   if the param is an invalid tuple
                    this is common in older collections
        """
        if not a_tag_tuple:
            raise ValueError("Tag Tuple Expected")
        if type(a_tag_tuple) not in self.__valid_tag_dtypes:
            raise DataTypeError(f"Tuple (or List) Expected: {a_tag_tuple}")

        return True

    def _extract_results_from_field(self,
                                    key_field: str,
                                    field: dict) -> list:
        if "tags" not in field:
            return []

        results = []

        def _analyze_tags(a_tag_type: str):
            tag_tuples = [x for x in field["tags"][a_tag_type]
                          if self._validate(x)]

            for tag_tuple in tag_tuples:
                results.append(self._build_result(field=field,
                                                  key_field=key_field,
                                                  a_tag_name=tag_tuple[0],
                                                  a_tag_score=tag_tuple[1],
                                                  a_tag_type=a_tag_type))

        for tag_type in ["supervised", "unsupervised"]:
            if field["tags"][tag_type] is not None:  # 1124#issuecomment-15320883
                _analyze_tags(tag_type)

        return results

    def process(self) -> DataFrame or None:

        results = []
        for record in self._records:
            key_field = record["key_field"]
            for field in record["fields"]:
                results += self._extract_results_from_field(field=field,
                                                            key_field=key_field)

        df_evidence = pd.DataFrame(results)

        if self._is_debug:
            self.logger.debug("\n".join([
                "Evidence API Complete",
                "\n{}".format(tabulate(df_evidence,
                                       headers='keys',
                                       tablefmt='psql'))]))

        if df_evidence.empty:
            self.logger.warning("No Evidence Found")
            return None

        return df_evidence
