#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pprint
from random import randint

import pandas as pd
from pandas import DataFrame

from base import BaseObject
from base import MandatoryParamError
from dataingest import IngestDataTransform
from datamongo import BaseMongoClient
from datamongo import CendantCollection


class GenerateSrcCollection(BaseObject):
    """ Loads Patent Data into a 'patent_src_<date>' collection
        Reference DataFlow:
            https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1089#issuecomment-15529935
    """

    def __init__(self,
                 df_join: DataFrame,
                 target_collection_name: str,
                 mongo_client: BaseMongoClient,
                 is_debug: bool = False):
        """
        Created:
            24-Oct-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/-cdo/unstructured-analytics/issues/1196
        :param df_join:
            the joined DataFrame
            this DataFrame is a combination of
            1.  IBM Patent Data from the BluePages API
            2.  USPTO Patent Data
        """
        BaseObject.__init__(self, __name__)
        if df_join.empty:
            raise MandatoryParamError("DataFrame")

        self._df_join = df_join
        self._is_debug = is_debug
        self._mongo_client = mongo_client
        self._target_collection_name = target_collection_name

    def process(self,
                flush_target_records: bool = True):
        from dataingest.patents.dmo import JoinedRecordTransformer

        target_records = []
        for _, row in self._df_join.iterrows():
            transformer = JoinedRecordTransformer(row, is_debug=self._is_debug)
            target_records.append(transformer.process())

        target_records = IngestDataTransform(target_records).process()

        target_collection = CendantCollection(is_debug=self._is_debug,
                                              some_base_client=self._mongo_client,
                                              some_collection_name=self._target_collection_name)

        if flush_target_records:
            target_collection.delete()

        target_collection.insert_many(target_records)

        random_record = randint(0, len(target_records) - 1)
        self.logger.debug("\n".join([
            "Patent Ingestion Activity Completed",
            "\tRandom Record({}):".format(random_record),
            pprint.pformat(target_records[random_record], indent=4)]))


if __name__ == "__main__":
    # this --main-- statement will be removed
    # the orchestrator (in the bp package) will use
    # the output of the join-service to call this service
    # in general, we shouldn't have any need to persist an intermediate df-join.tsv file

    infile = "/Users/craig.trimibm.com/Downloads/patent_join.tsv"
    local_df_join = pd.read_csv(infile, sep='\t', encoding='utf-8')

    GenerateSrcCollection(df_join=local_df_join,
                          target_collection_name='ingest_patents',
                          mongo_client=BaseMongoClient(),
                          is_debug=True).process(flush_target_records=True)
