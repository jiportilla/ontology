#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from collections import Counter

import pandas as pd

from base import BaseObject
from datamongo import BaseMongoClient
from datamongo import CendantXdm
from datamongo import CollectionFinder


class FindDimensionRecordsByRegion(BaseObject):
    """ Create a DataFrame Histogram of Regions in a Dimensions Schema """

    def __init__(self,
                 source_name: str,
                 mongo_client: BaseMongoClient = None,
                 is_debug: bool = False):
        """
        Created:
            21-May-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/243
        """
        BaseObject.__init__(self, __name__)
        self.is_debug = is_debug
        if not mongo_client:
            mongo_client = BaseMongoClient()

        self.source_name = source_name
        self.mongo_client = mongo_client

    def process(self):
        _dimensions = CendantXdm(is_debug=self.is_debug,
                                    mongo_client=self.mongo_client,
                                    collection_name=CollectionFinder.find_xdm(self.source_name))

        records = _dimensions.all()
        if self.is_debug:
            self.logger.debug("\n".join([
                "Retrieved Records (total={})".format(
                    len(records))]))

        c = Counter()
        for record in records:
            c.update({record["fields"]["region"]: 1})

        results = []
        for x in c:
            results.append({"Region": x, "Count": c[x]})
        return pd.DataFrame(results).sort_values(by=['Count'],
                                                 ascending=False)
