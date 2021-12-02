#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import sys

from base import BaseObject
from base import MandatoryParamError


class SlotValueFilter(BaseObject):
    """ Perform Record filtering based on the the sum of values in Dimension 'Slots'

        For example:
            {   cloud:              { value: 0 },
                data science:       { value: 1 },
                database:           { value: 0 },
                hard skill:         { value: 0 },
                soft skill:         { value: 2 },
                project management: { value: 0 } }
        the value sum = 3
    """

    _records = None

    def __init__(self,
                 some_records: list):
        """
        Created:
            1-May-2019
            craig.trim@ibm.com
            *   refactored out of '-demand-dimensions'
        """
        BaseObject.__init__(self, __name__)
        if not some_records:
            raise MandatoryParamError("Records")

        self.records = some_records

    def process(self,
                minimum_value_sum: int = None,
                maximum_value_sum: int = None,
                key_fields_only: bool = False) -> list:
        """
        Purpose:
            filter records by the sum of values
        :param minimum_value_sum:
            the minimum sum that is acceptable
            None    no lower limit
        :param maximum_value_sum:
            the maximum sum that is acceptable
            None    no upper limit
        :param key_fields_only:
            if True only return the key field values
        :return:
            a list of records that meet the defined thresholds
        """

        if not minimum_value_sum:
            minimum_value_sum = 0
        if not maximum_value_sum:
            maximum_value_sum = sys.maxsize

        def _sum(a_record: dict) -> int:
            values = []
            for slot in a_record["slots"]:
                values.append(int(a_record["slots"][slot]['weight']))

            return sum(values)

        records = []
        for record in self.records:
            _record_sum = _sum(record)
            if minimum_value_sum <= _record_sum <= maximum_value_sum:
                records.append(record)

        if not key_fields_only:
            return records
        return [x["key_field"] for x in records]
