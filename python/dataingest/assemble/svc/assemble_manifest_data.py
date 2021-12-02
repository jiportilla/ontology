#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import os
import pprint
from typing import Iterator

from pymongo.operations import UpdateOne
from pymongo.errors import BulkWriteError

from base import BaseObject
from base import MandatoryParamError
from dataingest.core.dmo import ManifestConnectorForMongo
from datamongo import BaseMongoClient
from datamongo import CendantCollection
from datamongo import CreateTextIndex

class BulkWriteResultTracker(object):
    def __init__(self):
        self.inserted_count = 0
        self.matched_count = 0
        self.modified_count = 0
        self.upserted_count = 0
    def update(self, result):
        self.inserted_count += result.inserted_count
        self.matched_count += result.matched_count
        self.modified_count += result.modified_count
        self.upserted_count += result.upserted_count

    def __str__(self):
        return f'matched={self.matched_count}. ' \
               f'inserted={self.inserted_count}. ' \
               f'upserted={self.upserted_count}. ' \
               f'modified={self.modified_count}'

class AssembleManifestData(BaseObject):
    """ Assemble all the Manifest Data from
            multiple source collections into a
            single target collection """

    def __init__(self,
                 some_manifest_name: str,
                 some_activity_name: str,
                 single_collection: str='',
                 first: int=-1,
                 last: int=-1,
                 is_debug: bool = False):
        """
        Created:
            12-Mar-2019
            craig.trim@ibm.com
        Updated:
            10-May-2019
            craig.trim@ibm.com
            *   added 'div-field'
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/236
        Updated:
            25-Jun-2019
            craig.trim@ibm.com
            *   added create-text-index
        Updated:
            16-Jul-2019
            craig.trim@ibm.com
            *   added debug param
            *   change collection name access method per
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/450
        Updated:
            1-Aug-2019
            craig.trim@ibm.com
            *   ensure consistent use of manifest-connector-for-mongo
            *   added 'bulk-insert-threshold' function and env var
            *   update logging
        Updated:
            15-Oct-2019
            craig.trim@ibm.com
            *   update index creation strategy
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1122
        Updated:
            24-Nov-2019
            xavier.verges@es.ibm.com
            *   more logging
            *   try to decrease the memory footprint by deleting a dict of
                large collections once we are done using it
        Updated:
            11-Dec-2019
            xavier.verges@es.ibm.com
            *   try to decrease the memory footprint by processing input collections
                one at a time, rather than having them all in memory at the same time
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1567
        Updated:
            07-Jan-2020
            xavier.verges@es.ibm.com
            *   use $addToSet instead of $push, to prevent re-adding the same fields
            *   allow to ignore field traceability (required for badges)
        :param some_manifest_name:
            the name of the manifest
        :param some_activity_name:
            the name of the activity within the manifest
        """
        BaseObject.__init__(self, __name__)
        from dataingest.core.dmo import ManifestActivityFinder

        if not some_manifest_name:
            raise MandatoryParamError("Manifest Name")
        if not some_activity_name:
            raise MandatoryParamError("Activity Name")

        self._is_debug = is_debug
        self._mongo_client = BaseMongoClient()
        self._threshold = self._bulk_insert_threshold()
        self._manifest = ManifestActivityFinder(some_manifest_name,
                                                some_activity_name).process()
        self._single_collection = single_collection
        self._first = first
        self._last = last
        self._log_hint = '' if not single_collection else f'{single_collection}_{first}-{last} '

    def _bulk_insert_threshold(self,
                               default_value: int = 1000):
        """
        :param default_value:
            default insert threshold if the environment variable isn't set
        :return:
            the bulk insert amount for mongoDB
        """
        try:
            return int(os.environ["ASSEMBLE_API_BULK_INSERT_THRESHOLD"])
        except KeyError as err:
            self.logger.error(f"Environment Variable Not Found: "
                              f"$ASSEMBLE_API_BULK_INSERT_THRESHOLD")
        except ValueError as err:
            self.logger.error(f"Invalid Environment Variable Value: "
                              f"$ASSEMBLE_API_BULK_INSERT_THRESHOLD")
        return default_value

    def source_collections(self) -> list:
        sources = []
        for name, value in self._source_collections().items():
            sources.append((name, value['collection'].count()))
        return sources

    def flush_target(self, target_collection=None) -> None:
        if not target_collection:
            target_collection = self._target_collection()
        target_collection.delete(keep_indexes=False)

    def index_target(self, target_collection=None) -> None:
        if not target_collection:
            target_collection = self._target_collection()
        index_options = {
            'background': True
        }
        self.logger.debug("Creating text indexes...")
        text_index_creator = CreateTextIndex(target_collection.collection, is_debug=True)
        text_index_creator.process(field_name=self._manifest['target']['index'],
                                   index_options=index_options)

    def _source_collections(self) -> dict:
        d = {}
        for source in self._manifest["sources"]:
            if self._single_collection and \
               self._single_collection != source['collection']:
               continue
            collection = ManifestConnectorForMongo(source,
                                                   some_base_client=self._mongo_client,
                                                   is_debug=self._is_debug).process()

            d[source["collection"]] = {"collection": collection,
                                       "fields": source["fields"],
                                       "div_field": source["div_field"],
                                       "key_field": source["key_field"]}
        return d

    def _target_collection(self) -> CendantCollection:

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Retrieved Target Manifest",
                pprint.pformat(self._manifest["target"])]))

        return ManifestConnectorForMongo(self._manifest["target"],
                                         some_base_client=self._mongo_client,
                                         is_debug=self._is_debug).process()

    def _load_records(self,
                      d_source_collections: dict) -> Iterator[dict]:
        """
        generator that loads one collection at a time
        :param d_source_collections:
            a dictionary of mongoDB collections
        :yields:
            a dictionary of records per collection
        """
        grand_total = 0

        for index, source_collection in enumerate(d_source_collections):
            collection = d_source_collections[source_collection]["collection"]


            d_records = {}
            if self._first < 0:
                records = collection.all()
                limit = collection.count()
            else:
                limit = self._last - self._first + 1
                records = collection.skip_and_limit(self._first, limit)
            self.logger.debug(f"Loading {limit} records from {source_collection}. "
                              f"Collection {index + 1} of {len(d_source_collections)}")
            d_records[source_collection] = {
                "records": records,
                "fields": d_source_collections[source_collection]["fields"],
                "div_field": d_source_collections[source_collection]["div_field"],
                "key_field": d_source_collections[source_collection]["key_field"]}

            total_records = len(d_records[source_collection]["records"])
            grand_total += total_records

            self.logger.debug(f"Loaded "
                              f"{total_records} total records "
                              f"from {source_collection}")
            yield d_records

        self.logger.debug(f"Loaded a grand total of "
                          f"{grand_total} records "
                          f"across {len(d_source_collections)} source collections")

    def process(self) -> None:
        from dataingest.core.dmo import SourceRecordMerger

        target_collection = self._target_collection()
        if not self._single_collection:
            self.flush_target(target_collection)

        fields_as_a_list = self._manifest['target'].get('allow_duplicate_fields', True)
        push_or_addToSet = '$push' if fields_as_a_list else '$addToSet'

        try:
            total_inserted = 0
            for d_records in self._load_records(self._source_collections()):
                d_index_by_key = SourceRecordMerger(d_records, field_traceability=fields_as_a_list).process()

                records = []
                bulk_res_tracker = BulkWriteResultTracker()

                for key_field in d_index_by_key:
                    fields = d_index_by_key[key_field]["fields"]
                    div_field = d_index_by_key[key_field]["div_field"]
                    d_index_by_key[key_field] = None

                    records.append(UpdateOne({'_id': key_field},
                                            {
                                                '$set': {
                                                    '_id': key_field,
                                                    'key_field': key_field,
                                                    'div_field': div_field
                                                },
                                                push_or_addToSet: {'fields': {'$each': fields}}
                                            },
                                            upsert=True))

                    if len(records) % self._threshold == 0:
                        result = target_collection.collection.bulk_write(records, ordered=False)
                        bulk_res_tracker.update(result)
                        self.logger.debug(f"Progress {self._log_hint}"
                                        f"{bulk_res_tracker} of {len(d_index_by_key)}")
                        records = []

                if len(records):
                    result = target_collection.collection.bulk_write(records, ordered=False)
                    bulk_res_tracker.update(result)
                    self.logger.debug(f"Progress {self._log_hint}"
                                    f"{bulk_res_tracker} of {len(d_index_by_key)}")
                total_inserted += (bulk_res_tracker.modified_count + bulk_res_tracker.upserted_count)
                self.logger.debug(f'DONE {self._log_hint} {bulk_res_tracker}')

            if not self._single_collection:
                self.index_target(target_collection)

            self.logger.debug(f"Assembled {total_inserted} records {self._log_hint}")

        except BulkWriteError as xcpt:
            self.logger.error(xcpt.details)
            raise
