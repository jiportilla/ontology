# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from pandas import DataFrame

from base import BaseObject
from dataingest import ManifestActivityFinder
from datamongo import BaseMongoClient
from datamongo import CendantCollection


class CvAssemblerByCnum(BaseObject):
    """
    It would be more convenient to simply use the "supply_complete" collection
    but the parsed collections are rarely in a complete state
    """

    def __init__(self,
                 some_df: DataFrame,
                 some_base_client: BaseMongoClient = None):
        """
        Created:
            4-Apr-2019
            craig.trim@ibm.com
        """
        BaseObject.__init__(self, __name__)
        if not some_base_client:
            some_base_client = BaseMongoClient()

        self.df = some_df
        self.base_client = some_base_client

    def _cnums(self):
        def _cleanse(some_cnum: str) -> str:
            some_cnum = str(some_cnum).replace("'", "")
            return some_cnum

        return [_cleanse(x) for x in self.df.cnum.unique()]

    def _collections(self) -> dict:
        d_collections = {}
        d_manifest = ManifestActivityFinder(some_manifest_name="assemble-manifest-supply",
                                            some_manifest_activity="Supply Complete").process()

        for source in d_manifest["sources"]:
            col = CendantCollection(some_base_client=self.base_client,
                                    some_db_name="cendant",
                                    some_collection_name=source["collection"])
            d_collections[source["collection"]] = col

        return d_collections

    def _results(self,
                 cnums: list,
                 d_collections: dict) -> dict:
        d_results = {}
        for collection_name in d_collections:

            col = self._collections()[collection_name]
            self.logger.debug("\n".join([
                "Processing Collection: {}".format(
                    collection_name)]))

            for cnum in cnums:

                record = col.by_field("fields.value", cnum)
                if not record:
                    continue

                if cnum not in d_results:
                    d_results[cnum] = {}
                if collection_name not in d_results[cnum]:
                    d_results[cnum][collection_name] = []
                d_results[cnum][collection_name].append(record)

        return d_results

    def process(self) -> dict:
        cnums = self._cnums()

        d_records = {}

        for collection_name in self._collections():
            self.logger.debug("\n".join([
                "Analyzing Collection (name={})".format(
                    collection_name)]))

            col = CendantCollection(some_base_client=self.base_client,
                                    some_db_name="cendant",
                                    some_collection_name=collection_name)

            for cnum in cnums:
                record = col.by_field(field_name="fields.value",
                                      field_value=cnum)
                if cnum not in d_records:
                    d_records[cnum] = {}
                if collection_name not in d_records[cnum]:
                    d_records[cnum][collection_name] = []
                if record:
                    d_records[cnum][collection_name].append(record)

        return d_records
