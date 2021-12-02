#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import time
from typing import Optional

from tabulate import tabulate

from base import BaseObject
from datamongo import BaseMongoClient


class ProcessMultipleRecords(BaseObject):
    """ dimensionality """

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
            *   refactored out of 'dimension-computation-orchestrator'
        Updated:
            13-May-2019
            craig.trim@ibm.com
            *   added 'name' to dimension-record
        Updated:
            17-Sept-2019
            craig.trim@ibm.com
            *   update initial debug statement
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/948
        Updated:
            15-Oct-2019
            craig.trim@ibm.com
            *   update param list to remove collection-category and use
                collection-name-tag and collection-name-xdm
        Updated:
            29-Oct-2019
            craig.trim@ibm.com
            *   remove 'entity-schema-finder' in favor of new approach
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/916#issuecomment-15620801
        Updated:
            16-Jan-2020
            xavier.verges@es.ibm.com
            *   remove the use of df_manifest, to prevent inconsistencies with
                the rest of parameters
            *   remove the use of the memory hungry RecordPager and
                use instead xdm_writer.exists()
            *   some general cleanup of unused code
        :param d_manifest:
            unused - collection names and schema are specified via parameter
        :param xdm_schema:
            the name of the schema to perform the type lookup
            Notes:
            -   typically either 'supply' or 'learning'
            -   the full list is on path 'resources/config/dimensionality'
        :param collection_name_tag:
        :param collection_name_xdm:
        :param mongo_client:
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)
        from cendantdim.batch.svc import ProcessSingleRecord

        if not mongo_client:
            mongo_client = BaseMongoClient()

        self._is_debug = is_debug
        self._mongo_client = mongo_client
        self._collection_name_tag = collection_name_tag
        self._collection_name_xdm = collection_name_xdm

        self._record_processor = ProcessSingleRecord(d_manifest=None,
                                                     xdm_schema=xdm_schema,
                                                     mongo_client=self._mongo_client,
                                                     collection_name_tag=collection_name_tag,
                                                     collection_name_xdm=collection_name_xdm,
                                                     is_debug=is_debug)

        self.logger.debug("\n".join([
            "Instantiated ProcessMultipleRecords",
            f"\tSchema: {xdm_schema}",
            f"\tURL: {mongo_client.url}",
            f"\tSource Collection: {collection_name_tag}",
            f"\tTarget Collection: {collection_name_xdm}"]))

    def flush_target(self) -> None:
        from datamongo.core.bp import CendantCollection
        collection = CendantCollection(some_collection_name=self._collection_name_xdm,
                                       some_base_client=self._mongo_client)
        collection.delete(keep_indexes=False)

    def _process_source_records(self,
                                source_records: list,
                                persist_threshold: int = 10) -> None:
        from cendantdim.batch.dmo import DimensionPersistence

        xdm_writer = DimensionPersistence(cendant_xdm=self._record_processor.cendant_xdm(),
                                          is_debug=self._is_debug)

        d_results_buffer = {}
        d_source_records = {}

        ctr = 0
        for source_record in source_records:
            ctr += 1
            start = time.time()

            key_field = source_record["key_field"]

            if xdm_writer.exists(key_field):
                self.logger.debug(f'xdm record {key_field} already there. Skipping')
                continue

            df_result, _ = self._record_processor.compute(source_record)

            if df_result is None:
                if self._is_debug:
                    self.logger.debug(f"Empty DataFrame "
                                      f"(key-field={key_field}, "
                                      f"collection={self._collection_name_tag})")
                continue

            if self._is_debug:
                self.logger.debug("\n".join([
                    "\n{}".format(tabulate(df_result,
                                           headers='keys',
                                           tablefmt='psql'))]))

            d_results_buffer[key_field] = df_result
            d_source_records[key_field] = source_record

            if len(d_results_buffer) >= persist_threshold:
                xdm_writer.insert_many(d_source_records,
                                       d_results_buffer)
                d_results_buffer = {}

            self.logger.debug(f"Processed Source Record: "
                              f"(key-field={key_field}, "
                              f"status={len(d_results_buffer)}-{ctr}-{len(source_records)}, "
                              f"time={round(time.time() - start, 2)})")

        if len(d_results_buffer):
            xdm_writer.insert_many(d_source_records,
                                   d_results_buffer)

    def by_record_paging(self,
                         start_record=None,
                         end_record=None,
                         flush_records=False):
        """
        Purpose:
            the consumer passes a start-record and end-record
            e.g., (0, 999) will process the first 1000 records
            this method will perform persistence of the results
        :param start_record:
            the start record to process
        :param end_record:
            the end record to process
        :param flush_records:
            True        flush existing records
        :return:
            None
        """
        from datamongo.core.bp import CendantCollection

        if flush_records:
            self.flush_target()

        collection = CendantCollection(some_collection_name=self._collection_name_tag,
                                       some_base_client=self._mongo_client)

        limit = end_record - start_record + 1
        source_records = collection.skip_and_limit(start_record, limit)

        if not len(source_records) and self._is_debug:
            self.logger.debug("No Further Records to Process")
            return

        self._process_source_records(source_records)

    def get_sources(self) -> list:
        from datamongo.core.bp import CendantCollection

        collection = CendantCollection(some_collection_name=self._collection_name_tag,
                                       some_base_client=self._mongo_client)
        return [(collection.collection_name, collection.count())]
