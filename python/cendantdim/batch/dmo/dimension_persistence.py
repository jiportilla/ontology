#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from pandas import DataFrame

from base import BaseObject
from cendantdim.batch.dto import DataFrameTransformer
from datamongo import CendantCollectionCategory
from datamongo import CendantXdm


class DimensionPersistence(BaseObject):
    """ Persist XDM records """

    def __init__(self,
                 cendant_xdm: CendantXdm,
                 is_debug=False):
        """
        Created:
            7-Aug-2019
            craig.trim@ibm.com
            *   refactored out of 'process-single-record'
        Updated:
            16-Jan-2020
            xavier.verges@es.ibm.com
            *   added method exists()
            *   added key_field value as `_id`
        """
        BaseObject.__init__(self, __name__)

        self._is_debug = is_debug
        self._cendant_xdm = cendant_xdm

        self._collection_name = self._cendant_xdm.collection.collection_name
        self._collection_category = CendantCollectionCategory.find(self._collection_name)

    def exists(self,
               key_field_value: str) -> bool:
        """
        Check if a record already exists
        """
        return self._cendant_xdm.collection.collection.find_one(filter={'_id': key_field_value}) is not None

    def insert_many(self,
                    d_source_records: dict,
                    d_results: dict) -> None:
        """
        Purpose:
            Persist a Dictionary of DataFrame Weights to MongoDB
        :param d_source_records:
        :param d_results:
            {   key_field-1: { <dataframe> },
                key_field-2: { <dataframe> },
                ...
                key_field-n: { <dataframe> }}
        :return:
        """
        from cendantdim import DataFrameTransformer

        def _df_to_dict(a_key_field: str):
            source_record = d_source_records[a_key_field]
            d_attrs = DataFrameTransformer.dimensionality_attributes(a_source_record=source_record,
                                                                     collection_category=self._collection_category)
            df_schema_weights = d_results[a_key_field]
            record = DataFrameTransformer.df_zscores_to_dict(df_schema_weights,
                                                             **d_attrs)
            record['_id'] = record['key_field']
            return record

        results = [_df_to_dict(x) for x in d_results]

        if self._is_debug:
            self.logger.debug(f"Inserted Records "
                              f"(total={len(results)})")

        self._cendant_xdm.collection.insert_many(results,
                                                 ordered_but_slower=False,
                                                 augment_record=False)

    def insert_single(self,
                      source_record: dict,
                      df_schema_weights: DataFrame):
        d_attrs = DataFrameTransformer.dimensionality_attributes(a_source_record=source_record,
                                                                 collection_category=self._collection_category)

        record = DataFrameTransformer.df_schema_weights_to_dict(df_schema_weights,
                                                                **d_attrs)
        record['_id'] = record['key_field']

        self._cendant_xdm.collection.save(record)

        if self._is_debug:
            self.logger.debug(f"Persisted Record "
                              f"(mongo_url={self._cendant_xdm.mongo_client.url}, "
                              f"collection={self._collection_name}, "
                              f"key-field={source_record['key_field']})")
