#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from typing import Dict
import time

from base import BaseObject
from datamongo import CendantCollection


class ParseRecordsFromMongo(BaseObject):
    """  """

    def __init__(self,
                 source_records: list,
                 target_collection: CendantCollection,
                 is_debug: bool = False):
        """
        Created:
            12-Apr-2019
            craig.trim@ibm.com
            *   refactored out of 'parse-manifest-data'
        Updated:
            16-Apr-2019
            craig.trim@ibm.com
            *   refactored '_handle_record' in support of
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/97
        Updated:
            26-Jul-2019
            craig.trim@ibm.com
            *   renamed from 'parse-record-handler'
                to differentiate from 'parse-records-from-file'
        Updated:
            22-Jul-2019
            craig.trim@ibm.com
            *   multi-instantiation of badge and text field parsing to prevent tag duplication
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/796#issuecomment-14041159
        Updated:
            30-Sept-2019
            craig.trim@ibm.com
            *   added source-record-filter
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1026
        Updated:
            12-Oct-2019
            craig.trim@ibm.com
            *   sort by field count; this will process the smallest records first
        Updated:
            17-Dec-2019
            xavier.verges@es.ibm.com
            *   remove exception handling: if the task fails, kubernetes will restart it
            *   make target_collection a mandatory param
            *   use the key_field also as the _id field for better performance
        Updated:
            02-Jan-2020
            xavier.verges@es.ibm.com
            *   check that a record is not already saved before processing
                (similar to what IncrementalRecordRetriever did)
        """
        BaseObject.__init__(self, __name__)
        from dataingest.parse.dmo import BadgeFieldParser
        from dataingest.parse.dmo import SourceRecordFilter
        from dataingest.parse.dmo import LongTextFieldParser
        from dataingest.parse.dmo import ParseSentenceSegmentizer

        self._is_debug = is_debug
        self._target_collection = target_collection
        self._badge_field_parser = BadgeFieldParser(is_debug=False)
        self._text_field_parser = LongTextFieldParser(is_debug=False)
        self._sentencizer = ParseSentenceSegmentizer(is_debug=is_debug)
        self._source_records = SourceRecordFilter(source_records, is_debug).process()

        self.logger.debug("Instantiate ParseRecordsFromMongo")

    def _handle_record(self,
                       record: dict) -> dict:
        _fields = []

        ctr = 0
        total_fields = len(record["fields"])

        for field in record["fields"]:

            ctr += 1
            if self._is_debug:
                if ctr % 10 == 0:
                    self.logger.debug(f"Field Processing "
                                      f"({ctr} - {total_fields})")

            def _handle_field():
                if field["type"] == "badge":
                    return self._badge_field_parser.process(badge_field=field)
                elif field["type"] == "long-text":
                    return self._text_field_parser.process(parse_field=field)
                return field

            _fields.append(_handle_field())

        record["fields"] = _fields

        return record

    def process(self,
                threshold: int = 25) -> int:

        process_start = time.time()
        ctr = 0
        _records = []

        total_persisted = 0
        total_skipped = 0
        total_records = len(self._source_records)

        self.logger.debug(f"Initialize Process Loop "
                          f"(total-records={total_records})")

        def log_ids(records):
            self.logger.error(f'insert_many error for records {[record["_id"] for record in records]}')

        def is_record_already_persisted(id):
            return self._target_collection.collection.count_documents({'_id': id}) != 0

        d_records: Dict[int, list] = {}
        for source_record in self._source_records:
            total_fields = len(source_record["fields"])
            if total_fields not in d_records:
                d_records[total_fields] = []
            d_records[total_fields].append(source_record)

        for total_fields in d_records:
            self.logger.debug(f"Record Handling Process Started ("
                              f"total-fields={total_fields}, "
                              f"total-records={len(d_records[total_fields])})")

            for source_record in d_records[total_fields]:

                if is_record_already_persisted(source_record['key_field']):
                    total_skipped += 1
                    continue

                ctr += 1
                start = time.time()

                source_record = self._sentencizer.process(source_record)
                record = self._handle_record(source_record)

                record_time = round((time.time() - start), 2)
                total_time = round((time.time() - process_start), 2)
                self.logger.debug(f"Handle Record. "
                                  f"Still not persisted: {ctr}. Already persisted: {total_persisted}. "
                                  f"Skipped: {total_skipped}. Total records: {total_records}. "
                                  f"Time: record->{record_time}s total->{total_time}. Fields: {total_fields}. "
                                  f"Collection: {self._target_collection.collection_name}")

                record["_id"] = record["key_field"]
                record["meta"] = {"seconds_parsing": round(record_time)}

                _records.append(record)

                if ctr >= threshold and len(_records):
                    ctr = 0

                    self._target_collection.insert_many(_records,
                                                        ordered_but_slower=False,
                                                        augment_record=False,
                                                        max_attempts=3,
                                                        failure_logger=log_ids)
                    total_persisted += len(_records)
                    _records = []

        if len(_records):
            self._target_collection.insert_many(_records,
                                                ordered_but_slower=False,
                                                augment_record=False,
                                                max_attempts=3,
                                                failure_logger=log_ids)
            total_persisted += len(_records)

        self.logger.debug(f"ParseRecordsFromMongo "
                          f"Persisted: {total_persisted}. Skipped: {total_skipped}. Total records: {total_records}.")

        return total_persisted


if __name__ == "__main__":
    from datamongo import BaseMongoClient

    source = CendantCollection(some_collection_name="supply_src_20191114", some_base_client=BaseMongoClient())
    target = CendantCollection(some_collection_name="supply_tag_test", some_base_client=BaseMongoClient())

    sr = source.by_key_field("06251K744")

    parser = ParseRecordsFromMongo(source_records=[sr], target_collection=target, is_debug=True)
    parser.process()
