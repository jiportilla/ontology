#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from pandas import DataFrame

from base import BaseObject
from datamongo.collections import Employees
from datamongo.collections import BandRegion
from datamongo.core.bp import CendantCollection
from datamongo.core.dmo import BaseMongoClient
from datamongo.core.dmo import BaseMongoHelper


class SupplyDimensions(BaseObject):
    """ Collection Wrapper over MongoDB Collection
        for "supply_dimensions"   """

    _records = None

    def __init__(self,
                 some_base_client=None):
        """
        Created:
            17-Apr-2019
            craig.trim@ibm.com
        """
        BaseObject.__init__(self, __name__)
        if not some_base_client:
            some_base_client = BaseMongoClient()

        self.cendant_collection = CendantCollection(some_base_client=some_base_client,
                                                    some_db_name="cendant",
                                                    some_collection_name="supply_dimensions")

        self.base_client = some_base_client
        self.mongo_collection = self.cendant_collection.collection
        self.helper = BaseMongoHelper(self.mongo_collection)

        self.band_region = BandRegion(self.base_client)
        self._employees = Employees(self.base_client)

    def all(self) -> list:
        if not self._records:
            self._records = self.cendant_collection.all()
        return self._records

    def histogram(self) -> DataFrame:
        from datamongo.slots.dmo import DimensionFrequency
        return DimensionFrequency(self.all()).process(as_dataframe=True)

    def value_by_cnum(self):
        d_records = {}
        for record in self.all():
            values = []
            for slot in record["slots"]:
                values.append(record["slots"][slot])
            d_records[record["key_field"]] = values

        return d_records

    def weight_by_cnum(self):
        d_records = {}
        for record in self.all():
            values = []
            for slot in record["slots"]:
                values.append(record["slots"][slot]["weight"])
            d_records[record["key_field"]] = values

        return d_records

    def _dim_values_by_keyed_param(self,
                                   d_cnum_by_param: dict) -> dict:
        """
        :param d_cnum_by_param:
            e.g.    a dictionary of CNUMs keyed by Region or Band
                    these dictionaries are generated from BandRegion
        :return:
            a dictionary
        """
        d_value_by_cnum = self.value_by_cnum()

        d_param_values = {}
        for param in d_cnum_by_param:

            param_values = []
            for cnum in d_cnum_by_param[param]:
                if cnum in d_value_by_cnum:
                    param_values.append(d_value_by_cnum[cnum])
            d_param_values[param] = param_values

        return d_param_values

    def dim_values_by_region(self) -> dict:
        """
        :return:
            a dictionary keyed by region
                with lists of dimension values

            sample output:
                {   'AP':   [   6.00, 13.5, 0.00, 18.0, 10.5, 0.00, 10.5, 4.25 ],
                            [   1.00, 1.75, 1.00, 9.00, 14.5, 4.00, 14.5, 1.00 ],
                            ...
                            [   1.50, 4.50, 0.00, 4.00, 1.00, 1.00, 3.50, 0.00 ]}
        """
        d = self.band_region.region_by_cnum(reverse=True)
        return self._dim_values_by_keyed_param(d)

    def dim_values_by_band(self) -> dict:
        """
        :return:
            a dictionary keyed by band
                with lists of dimension values

            sample output:
                {   '07':   [   6.00, 13.5, 0.00, 18.0, 10.5, 0.00, 10.5, 4.25 ],
                            [   1.00, 1.75, 1.00, 9.00, 14.5, 4.00, 14.5, 1.00 ],
                            ...
                            [   1.50, 4.50, 0.00, 4.00, 1.00, 1.00, 3.50, 0.00 ]}
        """
        d = self.band_region.band_by_cnum(reverse=True)
        return self._dim_values_by_keyed_param(d)

    def dim_values_by_region_and_band(self) -> dict:
        """
        :return:
            a dictionary keyed by band
                with lists of dimension values

            sample output:
                {   '07':   [   6.00, 13.5, 0.00, 18.0, 10.5, 0.00, 10.5, 4.25 ],
                            [   1.00, 1.75, 1.00, 9.00, 14.5, 4.00, 14.5, 1.00 ],
                            ...
                            [   1.50, 4.50, 0.00, 4.00, 1.00, 1.00, 3.50, 0.00 ]}
        """
        d_cnum_by_region = self.band_region.region_by_cnum(reverse=True)
        d_band_by_cnum = self.band_region.band_by_cnum(reverse=False)

        d_value_by_cnum = self.value_by_cnum()

        d_region = {}
        for region in d_cnum_by_region:

            if region not in d_region:
                d_region[region] = {}

            for cnum in d_cnum_by_region[region]:

                if cnum not in d_value_by_cnum:
                    continue

                band = d_band_by_cnum[cnum]
                if band not in d_region[region]:
                    d_region[region][band] = []

                d_region[region][band].append(d_value_by_cnum[cnum])

        return d_region

    def by_value_sum(self,
                     minimum_value_sum: int = None,
                     maximum_value_sum: int = None,
                     key_fields_only: bool = False) -> list:
        from datamongo.slots.dmo import SlotValueFilter
        return SlotValueFilter(some_records=self.all()).process(minimum_value_sum=minimum_value_sum,
                                                                maximum_value_sum=maximum_value_sum,
                                                                key_fields_only=key_fields_only)

    def reverse_index(self,
                      slot_name: str) -> DataFrame:
        from datamongo.slots.dmo import ReverseSlotIndex
        return ReverseSlotIndex(some_records=self.all(),
                                some_slot_name=slot_name).process(sort_ascending=True)


if __name__=="__main__":
    r = SupplyDimensions().by_value_sum(100, 0, True)
    print (len(r))
