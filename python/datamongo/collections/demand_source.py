#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pprint
import time

import pandas as pd
from pandas import DataFrame

from base import BaseObject
from base import RecordUnavailableRecord
from datamongo.core.bp import CendantCollection
from datamongo.core.dmo import BaseMongoClient


class DemandSource(BaseObject):
    """ Collection Wrapper over MongoDB Collection for "demand_src"   """

    _records = None

    def __init__(self,
                 some_base_client=None,
                 is_debug: bool = False):
        """
        Created:
            13-May-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/243
            *   merged 'ibm-openseats'
        """
        BaseObject.__init__(self, __name__)
        if not some_base_client:
            some_base_client = BaseMongoClient()

        self.is_debug = is_debug
        self.collection = CendantCollection(some_base_client=some_base_client,
                                            some_db_name="cendant",
                                            some_collection_name="demand_src")

    def all(self,
            limit: int = None):
        if not self._records:
            def _records():
                if not limit:
                    return self.collection.all()
                return self.collection.skip_and_limit(0, limit)

            self._records = _records()
            self.logger.debug("\n".join([
                "Retrieved Records",
                "\tTotal: {}".format(len(self._records))]))

        return self._records

    @staticmethod
    def to_dataframe(records: list) -> DataFrame:
        results = []

        for record in records:
            results.append({
                "Agent": record["agent"],
                "Type": record["type"],
                "Name": record["name"],
                "Value": record["value"],
                "Collection": record["collection"]})

        return pd.DataFrame(results)

    def band_range(self,
                   key_field: str) -> dict:

        def _low_band(a_record: dict) -> int:
            for field in a_record["fields"]:
                if field["name"] == "low_band":
                    return int(field["value"])

        def _high_band(a_record: dict) -> int:
            for field in a_record["fields"]:
                if field["name"] == "high_band":
                    return int(field["value"])

        record = self.collection.by_key_field(key_field)  # GIT-1415-16099357
        if not record:
            raise RecordUnavailableRecord("\n".join([
                "Demand Record Not Found (key-field={})".format(
                    key_field)]))

        svcresult = {
            "low": _low_band(record),
            "high": _high_band(record)}

        if self.is_debug:
            self.logger.debug("\n".join([
                "Retrieved Band Range (key-field={}, band={})".format(
                    key_field, svcresult)]))

        return svcresult

    @staticmethod
    def _status_query(status: str) -> dict:
        """
        Purpose:
            construct a date-range query for OpenSeats
        :param status:
            must be either (open, draft, withdrawn, closed)
        :return:
            a mongoDB query
        """

        return {
            'fields': {
                '$elemMatch': {
                    'name': "status",
                    'value': status
                }
            }
        }

    def by_date_range(self,
                      start_date: str,
                      end_date: str):
        """
        Retrieve OpenSeats by Date
        :param start_date:
            must be in format: YYYY-MM-DD
        :param end_date:
            must be in format: YYYY-MM-DD
        :return:
        """
        from datamongo.core.dmo import DateRangeQueryHelper
        query = DateRangeQueryHelper(start_date=start_date,
                                     end_date=end_date,
                                     is_debug=self.is_debug).process()

        start = time.time()
        results = self.collection.find_by_query(query)

        if self.is_debug:
            self.logger.debug("\n".join([
                "OpenSeats Returned By Date Range",
                "\tStart Date: {}".format(start_date),
                "\tEnd Date: {}".format(end_date),
                "\tTotal Records: {}".format(len(results)),
                "\tTotal Time: {}s".format(round(time.time() - start, 3)),
                "\t{}".format(pprint.pformat(query))]))

        return results

    def by_status(self,
                  status: str) -> list:
        """
        Retrieve OpenSeats by Status
        :param status:
            must be either (open, draft, withdrawn, closed)
        :return:
        """
        query = self._status_query(status)

        start = time.time()
        results = self.collection.find_by_query(query)

        if self.is_debug:
            self.logger.debug("\n".join([
                "OpenSeats Returned By Status",
                "\tStatus: {}".format(status),
                "\tTotal Records: {}".format(len(results)),
                "\tTotal Time: {}s".format(round(time.time() - start, 3)),
                "\t{}".format(pprint.pformat(query))]))

        return results

    def by_date_range_and_statuses(self,
                                   start_date: str,
                                   end_date: str,
                                   statuses: list) -> list:
        """
        Retrieve OpenSeats by Date
        :param start_date:
            must be in format: YYYY-MM-DD
        :param end_date:
            must be in format: YYYY-MM-DD
        :param statuses:
            must be either (open, draft, withdrawn, closed)
        :return:
        """
        master = []

        def _query(a_status: str) -> list:
            return self.by_date_range_and_status(start_date=start_date,
                                                 end_date=end_date,
                                                 status=a_status)

        for status in statuses:
            [master.append(x) for x in _query(status)]

        return master

    def by_date_range_and_status(self,
                                 start_date: str,
                                 end_date: str,
                                 status: str) -> list:
        """
        Retrieve OpenSeats by Date
        :param start_date:
            must be in format: YYYY-MM-DD
        :param end_date:
            must be in format: YYYY-MM-DD
        :param status:
            must be either (open, draft, withdrawn, closed)
        :return:
        """
        # query = self._date_range_query(start_date,
        #                                end_date)
        from datamongo.core.dmo import DateRangeQueryHelper
        query = DateRangeQueryHelper(start_date=start_date,
                                     end_date=end_date,
                                     is_debug=self.is_debug).process()

        query['$and'].append(self._status_query(status))

        start = time.time()
        results = self.collection.find_by_query(query)

        def _total_records():
            if not results or not len(results):
                return 0
            return len(results)

        if self.is_debug:
            self.logger.debug("\n".join([
                "OpenSeats Returned By Date Range and Status",
                "\tStart Date: {}".format(start_date),
                "\tEnd Date: {}".format(end_date),
                "\tStatuses: {}".format(status),
                "\tTotal Records: {}".format(_total_records()),
                "\tTotal Time: {}s".format(round(time.time() - start, 3)),
                "\t{}".format(pprint.pformat(query))]))

        return results
