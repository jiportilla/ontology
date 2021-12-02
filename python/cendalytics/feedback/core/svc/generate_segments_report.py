#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from typing import Optional

import pandas as pd

from base import BaseObject
from datadict import FindDimensions


class GenerateSegmentsReport(BaseObject):
    """  """

    __dim_cache = {}

    def __init__(self,
                 records: list,
                 is_debug: bool = False):
        """
        Created:
            22-Nov-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1419#issuecomment-16184266
        :param records:
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)

        self._records = records
        self._is_debug = is_debug
        self._dim_finder = FindDimensions.sentiment(is_debug=False)

    @staticmethod
    def _tags(field: dict,
              confidence_threshold: float = 0.0) -> set:
        """
        Purpose:
            Extract all Tags from a Field by Confidence
        :param field:
            the field
        :param confidence_threshold:
            <float>     the minimum value to consider
            sentiment analysis does not require high-precision and relies heavily on long-distance formation
            hence the default recommendation is 0.0
        :return:
            a set of tags
        """
        return set([tag[0] for tag in field["tags"]["supervised"]
                    if tag[1] >= confidence_threshold])

    def _all_tags(self,
                  record: dict) -> set:
        """
        Purpose:
            Extract all Tags from a Record
        :param record:
            the record
            (a record is composed of 1..* fields)
        :return:
            a set of tags
        """
        tags = set()
        long_text_fields = [field for field in record["fields"] if field["type"] == "long-text"]
        for field in long_text_fields:
            tags = tags.union(self._tags(field))
        return tags

    def _dim_finder_cache(self,
                          tag: str) -> list:
        if tag in self.__dim_cache:
            return self.__dim_cache[tag]
        self.__dim_cache[tag] = self._dim_finder.find(tag)
        return self.__dim_cache[tag]

    def process(self,
                log_threshold: int = 1000):
        from cendalytics.feedback.core.dto import SentimentRecordStructure

        ctr = 0
        master = []

        total_records = len(self._records)

        for record in self._records:
            record_id = record["key_field"]

            ctr += 1
            if ctr % log_threshold == 0:
                self.logger.debug(f"Status: {ctr}-{total_records}")

            def str_by_name(field_name: str) -> Optional[str]:
                results = [field for field in record["fields"] if field["name"] == field_name]
                if results and len(results):
                    try:
                        value = results[0]['value']
                        if type(value) == list:
                            return ' '.join(value)
                        return str(value).strip()
                    except TypeError as e:
                        self.logger.error('\n'.join([
                            f"Unknown Value (error={e})",
                            f"\tValue: {results[0]}"]))
                        return "unknown"
                self.logger.warning(f"Field Not Found: {field_name}")

            def int_by_name(field_name: str) -> Optional[str]:
                results = [field for field in record["fields"] if field["name"] == field_name]
                if results and len(results):
                    try:
                        return results[0]['value'][0]
                    except TypeError as e:
                        self.logger.error('\n'.join([
                            f"Unknown Value (error={e})",
                            f"\tValue: {results[0]}"]))
                        return "unknown"
                self.logger.warning(f"Field Not Found: {field_name}")

            for tag in self._all_tags(record):
                for xdm in self._dim_finder_cache(tag):
                    master.append(SentimentRecordStructure.record(
                        record_id=record_id,
                        tag=tag,
                        schema=xdm,
                        tenure=int_by_name('tenure'),
                        region=str_by_name('region'),
                        country=str_by_name("country"),
                        leadership=str_by_name("leadership")))

        df = pd.DataFrame(master)

        self.logger.info('\n'.join([
            f"Segment Generation Report Complete "
            f"(size={len(df)})"]))

        return df
