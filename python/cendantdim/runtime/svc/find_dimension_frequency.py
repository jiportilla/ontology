#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pandas as pd
from pandas import DataFrame

from base import BaseObject
from datamongo import CendantXdm
from datamongo import DimensionFrequency


class FindDimensionFrequency(BaseObject):

    def __init__(self,
                 cendant_xdm: CendantXdm,
                 is_debug: bool = False):
        """
        Created:
            30-Apr-2019
            craig.trim@ibm.com
        Updated:
            8-Aug-2019
            craig.trim@ibm.com
            *   removed -dimensions in favor of cendant-xdm
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/674
        """
        BaseObject.__init__(self, __name__)

        self.is_debug = is_debug
        self._cendant_xdm = cendant_xdm

    def process(self,
                sort_ascending: bool = True,
                limit: int = None) -> DataFrame:

        def _records():
            if not limit:
                return self._cendant_xdm.all()
            return self._cendant_xdm.collection.skip_and_limit(0, limit)

        records = _records()
        if len(records) == 0:
            db_name = self._cendant_xdm.collection.db_name
            mongo_url = self._cendant_xdm.collection.base_client.url
            collection_name = self._cendant_xdm.collection.collection_name
            self.logger.warning(f"No Record Found: "
                                f"(host={mongo_url}, "
                                f"db={db_name}, "
                                f"collection={collection_name})")
            return pd.DataFrame([])

        self.logger.debug(f"Located Records "
                          f"(total={len(records)})")

        df = DimensionFrequency(records).process()
        if sort_ascending and len(df):
            df = df.sort_values(['Frequency'],
                                ascending=[False])

        return df
