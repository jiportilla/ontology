# !/usr/bin/env python
# -*- coding: UTF-8 -*-


import pprint

from pandas import DataFrame

from base import BaseObject
from base import FieldStructure


class MockRecordGenerator(BaseObject):
    """ For the regression test to be convincing, mock records are created
        these records have a structure identical to records within MongoDB
    """

    __records = []

    def __init__(self,
                 df_regression: DataFrame,
                 is_debug: bool = False):
        """
        Created:
            20-Aug-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/786
        Updated:
            23-Aug-2019
            craig.trim@ibm.com
            *   added tag-tuple transformation method on input
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/786#issuecomment-14069017
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._process(df_regression)

    def records(self) -> list:
        return self.__records

    @staticmethod
    def _tag_tuples(tags: str) -> list:
        """
        Purpose:
            Transform Test Input into Tag Tuples
        :param tags:
            the test input for tags and associated scores
            Sample Format:
                Blockchain#100, Distributed Ledger#90, Hyperledger#95, Open Source#50
            or
                Blockchain, Distributed Ledger, Hyperledger, Open Source
            if no '#' is provided, the function assumes no score is provided, and will instead
                provide a default score of 100 for each incoming tag
        :return:
            a list of tag tuples
            Sample Output:
                [   ('Blockchain', 100.0),
                    ('Distributed Ledger', 90.0),
                    ('Hyperledger', 95.0),
                    ('Open Source', 50.0)]
        """
        tags = [x.strip() for x in tags.split(',') if x]

        tag_tuples = []
        for tag in tags:
            if '#' not in tag:
                tag_tuples.append((tag, 100.0))
            else:
                tokens = tag.split('#')
                tag_name = tokens[0].strip()
                tag_score = float(tokens[1])
                tag_tuples.append((tag_name, tag_score))

        return tag_tuples

    def _process(self,
                 df_regression: DataFrame) -> None:
        """
        Purpose:
            Transform the Regression DataFrame into a mock MongoDB Records
        :param df_regression:
            a DataFrame containing the Regression Test for dimensions
        :return:
            a list of mock MongoDB records
        """

        for serial_number in df_regression['Cnum'].unique():
            self.logger.debug(f"Analyzing Regression for {serial_number}")

            df_regression_by_serial = df_regression[df_regression['Cnum'] == serial_number]

            fields = []
            for _, row in df_regression_by_serial.iterrows():
                field_value = row['Text']
                schema_name = row['Schema']
                field_type = row['Collection']
                field_name = row['Field']

                tag_tuples = self._tag_tuples(row['Tags'])

                fields.append(FieldStructure.generate_tag_field(
                    supervised_tags=tag_tuples,
                    agent_name="System",
                    field_type=field_type,
                    field_name=field_name,
                    field_value=[field_value],
                    field_value_normalized=[field_value.lower()],
                    collection=field_type,
                    transformations=['regression']))

            self.__records.append({
                "key_field": serial_number,
                "div_field": "",
                "fields": fields})

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Generated Records",
                pprint.pformat(self.__records, indent=4)]))
