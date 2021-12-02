#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import time
from typing import Optional, Tuple

from pandas import DataFrame

from base import BaseObject
from base import MandatoryParamError
from cendantdim.batch.dto import DataFrameTransformer
from datamongo import BaseMongoClient
from datamongo import CendantXdm


class ProcessSingleRecord(BaseObject):
    """ Process Dimensionality for a Single Record only """

    def __init__(self,
                 d_manifest: Optional[dict],
                 xdm_schema: str,
                 collection_name_tag: str,
                 collection_name_xdm: str,
                 mongo_client: BaseMongoClient = None,
                 is_debug: bool = False):
        """
        Created:
            26-Apr-2019
            craig.trim@ibm.com
            *   refactored out of dimension-computation-orchestrator
        Updated:
            15-May-2019
            craig.trim@ibm.com
            *   refactored logic into svc and dmo components
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/243#issuecomment-11507326
        Updated:
            24-Jul-2019
            craig.trim@ibm.com
            *   add additional logging in support of
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/134#issuecomment-12882752
        Updated:
            7-Aug-2019
            craig.trim@ibm.com
            *   removed -dimensions and other refactoring
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/649
            *   refactor out z-score-calculator
            *   remove unused 'compute-inference-2' method
        Updated:
            15-Oct-2019
            craig.trim@ibm.com
            *   modify instantiation to single-record-locator
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1116#issuecomment-15308894
            *   clean up logging
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1123#issuecomment-15320681
        Updated:
            29-Oct-2019
            craig.trim@ibm.com
            *   remove 'entity-schema-finder' in favor of new approach
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/916#issuecomment-15620801
        :param d_manifest:
            unused - collection names and schema are specified via parameter
        :param xdm_schema:
            the name of the schema to perform the type lookup
            Notes:
            -   typically either 'supply' or 'learning'
            -   the full list is on path 'resources/config/dimensionality'
        :param collection_name_tag:
            the tag collection name to search
        :param collection_name_xdm:
            the xdm collection name to search
        :param mongo_client
        :param is_debug
        """
        BaseObject.__init__(self, __name__)
        from cendantdim.batch.dmo import DimensionPersistence

        if not mongo_client:
            mongo_client = BaseMongoClient()
        if not xdm_schema:
            raise MandatoryParamError("XDM Schema")

        self._is_debug = is_debug
        self._xdm_schema = xdm_schema
        self._mongo_client = mongo_client
        self._collection_name_tag = collection_name_tag
        self._collection_name_xdm = collection_name_xdm

        self._cendant_xdm = CendantXdm(collection_name=self._collection_name_xdm,
                                       mongo_client=mongo_client,
                                       is_debug=is_debug)
        self._xdm_writer = DimensionPersistence(cendant_xdm=self._cendant_xdm,
                                                is_debug=self._is_debug)

        if self._is_debug:
            self.logger.debug(f"Instantiated ProcessSingleRecord: "
                              f"Schema={xdm_schema}, "
                              f"Collection={self._collection_name_xdm}")

    def cendant_xdm(self) -> CendantXdm:
        return self._cendant_xdm

    def _source_record(self,
                       key_field: str) -> dict:
        from cendantdim.batch.dmo import SingleRecordLocator

        locator = SingleRecordLocator(is_debug=self._is_debug,
                                      base_mongo_client=self._mongo_client,
                                      collection_name=self._collection_name_tag)

        return locator.process(key_field)

    def compute(self,
                source_record: dict) -> Tuple[DataFrame, DataFrame]:
        from nlusvc import EvidenceExtractor
        from cendantdim.batch.dmo import ZScoreCalculator
        from cendantdim.batch.dmo import InferenceComputer
        from cendantdim.batch.dmo import WeightComputer
        from cendantdim.batch.dmo import TimeDimensionCalculator
        from cendantdim.batch.dmo import AcademicDimensionCalculator

        # Step: and Compute the Dimensionality Dataframe
        df_evidence = EvidenceExtractor(some_records=source_record,
                                        xdm_schema=self._xdm_schema,
                                        is_debug=self._is_debug).process()
        if df_evidence is None:
            self.logger.debug("Evidence Extraction Failure")
            return None, None

        # Step: Infer additional tags
        df_inference = InferenceComputer(df_evidence=df_evidence,
                                         xdm_schema=self._xdm_schema,
                                         is_debug=self._is_debug).process()

        if df_inference is None:
            self.logger.debug("Inference Computation Failure")
            return None, None

        # Step: Compute Weights on tags
        df_schema_weights = WeightComputer(key_field=source_record['key_field'],
                                           df_evidence=df_evidence,
                                           df_inference=df_inference,
                                           xdm_schema=self._xdm_schema).process()
        if df_schema_weights is None:
            self.logger.debug("Schema Weight Computation Failure")
            return None, None

        # Step: Compute zScores
        df_zscores = ZScoreCalculator(df_schema_weights).process()
        if df_zscores is None:
            self.logger.debug("zScore Computation Failure")
            return None, df_schema_weights

        # Step: Compute the Academic Dimension
        df_academic = AcademicDimensionCalculator(source_record=source_record,
                                                  is_debug=self._is_debug).process().final()
        df_zscores = df_zscores.append(df_academic, ignore_index=True)

        # Step: Compute the Experience Dimension
        df_experience = TimeDimensionCalculator(source_record=source_record,
                                                is_debug=self._is_debug).process().final()
        df_zscores = df_zscores.append(df_experience, ignore_index=True)

        return df_zscores, df_schema_weights

    def process(self,
                key_field: str,
                persist_result: bool = True) -> Optional[DataFrame]:

        start = time.time()

        # Step: If the record is already in the Dimension Collection, use it
        record = self._cendant_xdm.collection.by_key_field(key_field)
        if record:
            df_result = DataFrameTransformer.dict_to_df_zscores(record)
            if self._is_debug:
                self.logger.debug(f"XDM Record Exists: "
                                  f"key-field={key_field}")
            return df_result

        # Step: Otherwise, find the Source Record
        source_record = self._source_record(key_field)
        time_1 = time.time()

        df_zscores, df_schema_weights = self.compute(source_record)
        time_2 = time.time()

        if persist_result:
            self._xdm_writer.insert_single(source_record=source_record,
                                           df_schema_weights=df_schema_weights)

        if self._is_debug:
            step_1 = round(time_1 - start, 2)
            step_2 = round(time_2 - time_1, 2)
            self.logger.debug(f"Single Record Profile "
                              f"(Step 1: {step_1}, "
                              f"Step 2: {step_2})")

        return df_zscores
