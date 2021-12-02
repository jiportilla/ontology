#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import os

import pandas as pd
from pandas import DataFrame

from base import BaseObject
from base import FieldStructure
from dataingest.feedback.dmo import RegionalRollupMapper
from dataingest.feedback.dmo import TenureValueMapper
from datamongo import CendantCollection


class IngestInternalFeedback(BaseObject):
    """ NOTE
        this input file will not exist on the server for reasons of confidentiality """

    __input_file = 'WW GTS Comments October 2019_For Peter.xlsx'

    def __init__(self,
                 collection_name: str,
                 is_debug: bool = False):
        """
        Created:
            22-Nov-2019
            craig.trim@ibm.com
            *   refactored out of 'generate-sentiment-graph'
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1419#issuecomment-16183547
        Updated:
            26-Nov-2019
            craig.trim@ibm.com
            *   add sentence segmentation
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1452
        Updated:
            16-Jan-2020
            craig.trim@ibm.com
            *   minor updates to logging and initialization
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1745
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)

        self._is_debug = is_debug
        self._collection_name = collection_name

        self._tenure_mapper = TenureValueMapper(is_debug=self._is_debug)
        self._region_mapper = RegionalRollupMapper(is_debug=self._is_debug)

    def _read(self) -> DataFrame:
        inpath = os.path.join(os.environ['CODE_BASE'],
                              'resources/confidential_input',
                              self.__input_file)
        df = pd.read_excel(inpath)

        self.logger.debug('\n'.join([
            "Feedback Spreadsheet Loaded",
            f"\Total Records: {len(df)}",
            f"\tInput Path: {self.__input_file}"]))

        return df

    def process(self):
        df_input = self._read()

        record_ctr = 0
        div_field = ''
        target_records = []

        for _, row in df_input.iterrows():
            fields = []

            tenure = self._tenure_mapper.lookup(row['tenure'])
            region = self._region_mapper.lookup(row['country'])

            fields.append(FieldStructure.generate_src_field(agent_name="user",
                                                            field_type="text",
                                                            field_name='market',
                                                            field_value=row['market'],
                                                            transformations=[]))

            fields.append(FieldStructure.generate_src_field(agent_name="user",
                                                            field_type="text",
                                                            field_name='country',
                                                            field_value=row['country'],
                                                            transformations=[]))

            fields.append(FieldStructure.generate_src_field(agent_name="user",
                                                            field_type="text",
                                                            field_name='region',
                                                            field_value=region,
                                                            transformations=['region-mapper']))

            fields.append(FieldStructure.generate_src_field(agent_name="user",
                                                            field_type="text",
                                                            field_name='leadership',
                                                            field_value=row['leadership'],
                                                            transformations=[]))

            fields.append(FieldStructure.generate_src_field(agent_name="user",
                                                            field_type="text",
                                                            field_name='tenure',
                                                            field_value=tenure,
                                                            transformations=['tenure-mapper']))

            fields.append(FieldStructure.generate_src_field(agent_name="user",
                                                            field_type="long-text",
                                                            field_name='comments',
                                                            field_value=row['what_else_to_share'],
                                                            transformations=[]))

            target_records.append({
                "fields": fields,
                "key_field": record_ctr,
                "div_field": div_field,
                "manifest": {
                    "name": self.__input_file}})

            record_ctr += 1

        collection = CendantCollection(some_collection_name=self._collection_name)
        collection.delete()
        collection.insert_many(target_records)


def main():
    # not going to make this more robust;
    # feedback ingestion is not considered a repeatable process
    IngestInternalFeedback(collection_name="feedback_src_20200116").process()


if __name__ == "__main__":
    main()
