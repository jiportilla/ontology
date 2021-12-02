#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pandas as pd
from pandas import DataFrame

from base import BaseObject
from base import MandatoryParamError


class ReverseSlotIndex(BaseObject):
    """ Create a Reverse Index for a given slot """

    _records = None

    def __init__(self,
                 some_records: list,
                 some_slot_name: str):
        """
        Created:
            1-May-2019
            craig.trim@ibm.com
            *   refactored out of -learning-dimensions
        """
        BaseObject.__init__(self, __name__)
        if not some_slot_name:
            raise MandatoryParamError("Records")
        if not some_slot_name:
            raise MandatoryParamError("Slot Name")

        self.records = some_records
        self.slot_name = some_slot_name

    def _results(self):
        results = []
        for record in self.records:

            for slot in record["slots"]:
                if slot != self.slot_name:
                    continue

                value = record["slots"][slot]
                if value <= 0:
                    continue

                results.append({"KeyField": record["key_field"],
                                "Value": value})

        return results

    def process(self,
                sort_ascending: bool = True) -> DataFrame:
        df = pd.DataFrame(self._results())
        if sort_ascending and len(df):
            df = df.sort_values(['Value'],
                                ascending=[False])

        return df
