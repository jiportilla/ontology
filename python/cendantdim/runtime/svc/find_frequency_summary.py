#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pandas as pd
from pandas import DataFrame

from base import BaseObject
from datamongo import BaseMongoClient
from datamongo import CendantXdm
from datamongo import CollectionFinder


class FindFrequencySummary(BaseObject):

    def __init__(self,
                 mongo_client: BaseMongoClient,
                 is_debug: bool = False):
        """
        Created:
            2-May-2019
            craig.trim@ibm.com
            *   refactored out of dimensions-api
        Updated:
            14-May-2019
            craig.trim@ibm.com
            *   updated param list
        Updated:
            8-Aug-2019
            craig.trim@ibm.com
            *   remove -dimemsions in favor of cendant-xdm
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/674
        """
        BaseObject.__init__(self, __name__)
        self.is_debug = is_debug

        self._supply_xdm = CendantXdm(is_debug=is_debug,
                                      mongo_client=mongo_client,
                                      collection_name=CollectionFinder.find_xdm("supply"))

        self._demand_xdm = CendantXdm(is_debug=is_debug,
                                      mongo_client=mongo_client,
                                      collection_name=CollectionFinder.find_xdm("demand"))

        self._learning_xdm = CendantXdm(is_debug=is_debug,
                                        mongo_client=mongo_client,
                                        collection_name=CollectionFinder.find_xdm("learning"))

    def process(self) -> DataFrame:
        """
        Purpose:
            create a dataframe with slot value frequency

        Example:
            Label      Evidence Total
            Supply     0        31973
            Demand     0        157600
            Learning   0        79600
            Supply     1        149
            Demand     1        0
        :return:
            pandas DataFrame
        """
        m = 0
        results = []

        while True:

            total_supply = len(self._supply_xdm.by_value_sum(m, m))
            results.append({"Evidence": m,
                            "Total": total_supply,
                            "Label": "Supply"})

            total_demand = len(self._demand_xdm.by_value_sum(m, m))
            results.append({"Evidence": m,
                            "Total": total_demand,
                            "Label": "Demand"})

            total_learning = len(self._learning_xdm.by_value_sum(m, m))
            results.append({"Evidence": m,
                            "Total": total_learning,
                            "Label": "Learning"})

            if total_supply == 0 and total_demand == 0 and total_learning == 0:
                break

            m += 1

        return pd.DataFrame(results)
