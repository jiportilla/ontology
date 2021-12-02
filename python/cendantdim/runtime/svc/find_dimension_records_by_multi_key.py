#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pandas as pd
from pandas import DataFrame

from base import BaseObject
from dataingest import ManifestActivityFinder
from datamongo import BaseMongoClient


class FindDimensionRecordsByMultiKey(BaseObject):

    def __init__(self,
                 mongo_client: BaseMongoClient = None,
                 is_debug: bool = False):
        """
        Created:
            15-May-2019
            craig.trim@ibm.com
        Updated:
            8-Aug-2019
            craig.trim@ibm.com
            *   updates to logging and documentation
        """
        BaseObject.__init__(self, __name__)
        if not mongo_client:
            mongo_client = BaseMongoClient()

        self._is_debug = is_debug
        self._mongo_client = mongo_client

    def process(self,
                source_name: str,
                xdm_schema: str,
                collection_name_tag: str,
                collection_name_xdm: str,
                key_fields: list) -> DataFrame:
        """
        Purpose:
            Return dimension records by key-field
        :param source_name:
            the source collection to search in
                e.g.,  Supply, Demand, Learning (case insensitive)
        :param xdm_schema:
            the name of the schema to perform the type lookup
            Notes:
            -   typically either 'supply' or 'learning'
            -   the full list is on path 'resources/config/dimensionality'
        :param collection_name_tag:
            the tag collection name to search
        :param collection_name_xdm:
            the xdm collection name to search
        :param key_fields:
            the key fields to search by
            e.g., Open Seat ID, Serial Number, Learning Activity ID
        :return:
            a DataFrame of evidence for the key-field(s) provided
                +----+----------------------+----------+----------+--------------+
                |    | Schema               |   Weight |   zScore |   zScoreNorm |
                |----+----------------------+----------+----------+--------------|
                |  0 | blockchain           |    0     |    -1.12 |          0   |
                |  1 | quantum              |    0     |    -1.12 |          0   |
                |  2 | cloud                |   43.53  |    -0.57 |          0.6 |
                |  3 | system administrator |   91.548 |     0.04 |          1.2 |
                |  4 | database             |   50.851 |    -0.47 |          0.7 |
                |  5 | data science         |  187.584 |     1.26 |          2.4 |
                |  6 | hard skill           |  247.011 |     2.01 |          3.1 |
                |  7 | other                |   62.47  |    -0.33 |          0.8 |
                |  8 | soft skill           |  161.646 |     0.93 |          2.1 |
                |  9 | project management   |   85.025 |    -0.04 |          1.1 |
                | 10 | service management   |   41.458 |    -0.59 |          0.5 |
                +----+----------------------+----------+----------+--------------+
        """
        from cendantdim.batch.svc import ProcessSingleRecord

        d_manifest = ManifestActivityFinder(some_manifest_name="dimension-manifest",
                                            some_manifest_activity=source_name).process()

        psr = ProcessSingleRecord(d_manifest=d_manifest,
                                  xdm_schema=xdm_schema,
                                  collection_name_tag=collection_name_tag,
                                  collection_name_xdm=collection_name_xdm,
                                  mongo_client=self._mongo_client,
                                  is_debug=self._is_debug)

        master = []
        for key_field in key_fields:
            df = psr.process(key_field=key_field,
                             persist_result=False)

            for _, row in df.iterrows():
                master.append({
                    "KeyField": key_field,
                    "Schema": row['Schema'],
                    "Weight": row['Weight'],
                    "zScore": row['zScore'],
                    "zScoreNorm": row['zScoreNorm']})

        df_final = pd.DataFrame(master)
        if self._is_debug:
            self.logger.debug(f"Located Dimension Records "
                              f"(total={len(key_fields)})")

        return df_final
