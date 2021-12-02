#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import time

from bs4 import BeautifulSoup

from base import BaseObject
from base import FieldStructure
from base import MandatoryParamError
from nlutext import ExperienceYearsExtractor

removals = ['"', "'", "´", "•", "・", "", "", "",
            ""]  # TO-DO: Make this more robust with https://stackoverflow.com/a/32540287/239408
replacements = {
    "\t": "",
    "\n": "",
    "\r": "",
    "&": " and "
}


class IngestDataTransform(BaseObject):
    """ transform data based on the manifest description """

    def __init__(self,
                 some_target_records: list):
        """
        Created:
            11-Mar-2019
            craig.trim@ibm.com
        Updated:
            26-Aug-2019
            craig.trim@ibm.com
            *   use field-structure object to generate field
        :param some_target_records:
            the target records that have already been mapped
        """
        BaseObject.__init__(self, __name__)
        if not some_target_records:
            raise MandatoryParamError("Target Records")

        self.target_records = some_target_records

    @staticmethod
    def _field(field_name: str,
               field_type: str,
               field_value: str,
               field_agent: str) -> dict:
        """
        Purpose:
            Create a field definition
        Sample Input:
            1. The Eye Lock2. Principles of Adult learning3. Methods of learning4.
        Sample Output:
            1. The Eye Lock 2. Principles of Adult learning 3. Methods of learning4.
        """

        # don't add 'na' << this is common for 'North America'
        null_values = ['(null)', 'none', 'n/a', '(none)', 'null', 'nan', 'None']
        if not field_value or field_value.lower() in null_values:
            return FieldStructure.generate_src_field(agent_name=field_agent,
                                                     field_type=field_type,
                                                     field_name=field_name,
                                                     field_value=None,
                                                     transformations=[])
        if "1." in field_value:
            for i in range(0, 10):
                tmp_1 = "{}.".format(str(i))
                tmp_2 = " {}. ".format(str(i))
                if tmp_1 in field_value:
                    field_value = field_value.replace(tmp_1, tmp_2)

        for removal in removals:
            if removal in field_value:
                field_value = field_value.replace(removal, "")

        for k in replacements:
            if k in field_value:
                field_value = field_value.replace(k, replacements[k])

        def _is_html() -> bool:
            return "<" in field_value and ">" in field_value

        if field_type == "long-text" and _is_html():
            """ add spaces for bs4 so that data like this
                    <h2>Abstract</h2><p>Text</p>
                is not extracted like this
                    AbstractText
                by adding spaces between tags, this
                    <h2> Abstract </h2> <p> Text </p>
                becomes
                    Abtract Text"""
            field_value = field_value.replace("<", " <")
            field_value = field_value.replace(">", "> ")
            field_value = BeautifulSoup(field_value,
                                        features="html.parser").get_text()

        while "  " in field_value:
            field_value = field_value.replace("  ", " ")

        return FieldStructure.generate_src_field(agent_name=field_agent,
                                                 field_type=field_type,
                                                 field_name=field_name,
                                                 field_value=field_value,
                                                 transformations=[])

    @staticmethod
    def _apply_transformations(some_field: dict):
        """ apply 1..* custom transformations to a field
        :param some_field:
        :return:
            the cleansed field value
        """
        from . import IngestTransformRoutines

        transformations = IngestTransformRoutines()

        # Step: Define a Generic Transformation
        def _transform(a_fn_transform_name: str,
                       a_field_value: str) -> str:
            """ apply a named transformation to a given value
            :param a_fn_transform_name:
            :param a_field_value:
            :return:
                the transformed value
            """
            fn = getattr(transformations,
                         a_fn_transform_name)
            return fn(a_field_value)

        # Step: Call Transformation Routine as Needed
        _field_value = some_field["value"]
        for fn_transform_name in some_field["transformations"]:
            _field_value = _transform(fn_transform_name,
                                      _field_value)

        # Step: Return Cleansed Value
        return _field_value

    def process(self) -> list:

        ctr = 0
        start = time.time()
        total_records = len(self.target_records)

        _records = []
        for record in self.target_records:

            _fields = []
            for field in record["fields"]:

                if not field["value"]:
                    continue

                # apply custom transformations
                if field["transformations"] and len(field["transformations"]):
                    field["value"] = self._apply_transformations(field)

                if type(field["value"] == list):
                    _fields.append(field)
                else:

                    # standard transformation
                    _fields.append(self._field(field["name"],
                                               field["type"],
                                               field["value"],
                                               field["agent"]))

                # apply year transformation
                if field["type"] == "long-text" and field["value"]:
                    years = ExperienceYearsExtractor(field["value"]).process()
                    if len(years):
                        field["tags"] = {
                            "years": years}

            record["fields"] = _fields
            _records.append(record)

            ctr += 1
            if ctr % 10000 == 0:
                self.logger.debug("\n".join([
                    f"Ingest Data Transformation: {ctr} - {total_records}"]))

        self.logger.debug("\n".join([
            "Transformed Target Records",
            f"\tTotal Time: {time.time() - start}",
            f"\tOriginal Records: {len(self.target_records)}",
            f"\tTransformed Records: {len(_records)}"]))

        return self.target_records
