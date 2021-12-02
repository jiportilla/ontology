#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import datetime

import pandas as pd
from pandas import DataFrame
from tabulate import tabulate

from base import BaseObject
from datamongo import BaseMongoClient
from datamongo import CendantCollection


class CountMongoCollections(BaseObject):
    """ Provide a convenient way to count the total records in MongoDB collections
    """

    def __init__(self,
                 is_debug: bool = False):
        """
        Created:
            4-Oct-2019
            craig.trim@ibm.com
        Updated:
            26-Nov-2019
            craig.trim@ibm.com
            *   add filter-by-date functionality
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug

    @staticmethod
    def _aggregate(names: list):
        return {
            "src": sorted([name for name in names if "src" in name]),
            "tag": sorted([name for name in names if "tag" in name]),
            "xdm": sorted([name for name in names if "xdm" in name])}

    @staticmethod
    def _filter(filter_name: str,
                names: list) -> list:
        return [name for name in names if name.startswith(filter_name)]

    @staticmethod
    def _filter_by_date(names: list) -> list:
        mydate = datetime.datetime.now()  # '2019-11-26 08:13:58.660388'
        tokens = str(mydate).split(' ')[0].split('-')  # ['2019', '11', '26']
        pattern = f"{tokens[0]}{tokens[1]}"  # '201911'
        return [name for name in names if pattern in name]

    def _find_names(self,
                    base_mongo_client: BaseMongoClient) -> dict:
        """
        Purpose:
            Generate a dictionary object that aggregates collections by type and stage
        Sample Output:
            {'demand': {'src': ['demand_src_20190913',
                                    ...
                                'demand_src_20190909'],
                        'tag': ['demand_tag_20190917',
                                    ...
                                'demand_tag_20191003'],
                        'xdm': ['demand_xdm_20190927']},
             'learning': {'src': ['learning_src_20190806',
                                    ...
                                  'learning_src_20191002'],
                          'tag': ['learning_tag_20190806',
                                    ...
                                  'learning_tag_20191004'],
                          'xdm': []},
             'supply': {'src': ['supply_src_20190801',
                                    ...
                                'supply_src_20190814'],
                        'tag': ['supply_tag_20190913',
                                    ...
                                'supply_tag_20190817'],
                        'xdm': ['supply_xdm_20190917',
                                    ...
                                'supply_xdm_20190807']}}
        :param base_mongo_client:
            an instantiated mongoDB client instance
        :return:
            a dictionary of collections
        """

        client = base_mongo_client.client
        names = sorted(dict((db, [collection for collection in client[db].collection_names()])
                            for db in client.database_names()).values())[0]

        names = self._filter_by_date(names)

        d_collections = {}
        for filter_name in ["supply", "demand", "learning", "feedback", "patent", "github"]:
            d_collections[filter_name] = self._aggregate(names=self._filter(filter_name, names=names))

        return d_collections

    def _count_sizes(self,
                     base_mongo_client: BaseMongoClient,
                     d_collections: dict) -> DataFrame:
        """
        Purpose:
            Count Collection Sizes and Generate DataFrame of output
        Sample Output:
            +----+------------+---------+-----------------------+----------+
            |    | Category   |   Count | Name                  | Type     |
            |----+------------+---------+-----------------------+----------|
            |  0 | src        |  207169 | supply_src_20190801   | supply   |
            |  1 | src        |  238246 | supply_src_20190814   | supply   |
            ...s
            | 40 | tag        |  174660 | learning_tag_20190923 | learning |
            | 41 | tag        |  169517 | learning_tag_20191004 | learning |
            +----+------------+---------+-----------------------+----------+

        :param base_mongo_client:
            an instantiated mongoDB client instance
        :param d_collections:
            output produced by the prior step
        :return:
            a DataFrame of output
        """

        results = []
        for collection_type in d_collections:
            for collection_category in d_collections[collection_type]:
                for collection_name in d_collections[collection_type][collection_category]:
                    total = CendantCollection(some_collection_name=collection_name,
                                              some_base_client=base_mongo_client).count()

                    if self._is_debug:
                        self.logger.debug(f"Collection Counted "
                                          f"(name={collection_name}, total={total})")

                    results.append({
                        "Type": collection_type,
                        "Category": collection_category,
                        "Name": collection_name,
                        "Count": total})

        return pd.DataFrame(results)

    def process(self):
        base_mongo_client = BaseMongoClient(is_debug=True)

        d_collections = self._find_names(base_mongo_client)
        df = self._count_sizes(base_mongo_client, d_collections)

        self.logger.debug('\n'.join([
            "Cendant Collection Counting Completed",
            tabulate(df,
                     headers='keys',
                     tablefmt='psql')]))


def main():
    CountMongoCollections().process()


if __name__ == "__main__":
    import plac

    plac.call(main)
