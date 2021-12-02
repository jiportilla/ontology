#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import sys
from collections import Counter

import pandas as pd
from tabulate import tabulate

from base import BaseObject
from datadict import FindDimensions
from datamongo import BaseMongoClient
from datamongo import CendantTag


class SchemaElementsFrequency(BaseObject):
    """
    Purpose:
        For a given collection, generate the Schemas and Tags with their relative frequencies by order of appearance
    Rationale:
        Useful for testing to continually hone in on 'unlisted' and  'other' categories
    Reference:
        https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1205#issuecomment-15589827
    Sample Output:
        +------+---------+-----------------------------+-------------------------------------------------------------------+
        |      |   Count | Schema                      | Tag                                                               |
        |------+---------+-----------------------------+-------------------------------------------------------------------|
        |    0 |    8496 | unlisted                    | ibm                                                               |
        |    1 |    5469 | unlisted                    | problem solving                                                   |
        |    2 |    4641 | unlisted                    | project                                                           |
        |    3 |    4399 | service management          | support                                                           |
        |    4 |    4361 | unlisted                    | implement                                                         |
        |    5 |    3827 | project management          | management                                                        |

        | 2158 |       1 | database                    | db2 bind                                                          |
        +------+---------+-----------------------------+-------------------------------------------------------------------+
    """

    def __init__(self,
                 xdm_schema: str,
                 collection_name_tag: str,
                 mongo_client: BaseMongoClient,
                 database_name: str = 'cendant',
                 is_debug: bool = False):
        """
        Created:
            28-Oct-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1205#issuecomment-15589827
        Updated:
            29-Oct-2019
            craig.trim@ibm.com
            *   remove 'entity-schema-finder' in favor of new approach
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/916#issuecomment-15620801
        Updated:
            31-Oct-2019
            xavier.verges@es.ibm.com
            *   renamed from TestSchemaElements to SchemaElementsFrequency so that py.test did not try to executes
        :param xdm_schema:
            the name of the schema to perform the type lookup
            Notes:
            -   typically either 'supply' or 'learning'
            -   the full list is on path 'resources/config/dimensionality'
        :param collection_name_tag:
            a MongoDB collection
        :param database_name:
            a MongoDB database
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)

        self._is_debug = is_debug
        self._dim_finder = FindDimensions(xdm_schema)

        self._collection = CendantTag(
            is_debug=self._is_debug,
            mongo_client=mongo_client,
            database_name=database_name,
            collection_name=collection_name_tag)

    def process(self,
                skip: int = None,
                limit: int = None,
                most_common: int = None):

        def _records():
            if skip or limit:
                return self._collection.collection.skip_and_limit(skip=skip, limit=limit)
            return self._collection.all()

        records = _records()

        if self._is_debug:
            self.logger.debug('\n'.join([
                f"Retrieved Records ("
                f"skip={skip}, "
                f"limit={limit}, "
                f"total-records={len(records)})"]))

        c = Counter()

        for record in records:
            for field in record["fields"]:
                if "tags" in field:
                    if "supervised" in field["tags"]:
                        for tag in field["tags"]["supervised"]:
                            c.update({tag[0]: 1})

        def _counter_elements() -> dict:
            if most_common:
                return dict(c.most_common(most_common))
            return dict(c.most_common(sys.maxsize))

        results = []
        d = _counter_elements()
        for tag in d:
            count = d[tag]
            for schema in self._dim_finder.find(tag):
                results.append({
                    "Tag": tag,
                    "Count": count,
                    "Schema": schema})

        df = pd.DataFrame(results)

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Schema Element Test Completed",
                tabulate(df, headers='keys', tablefmt='psql')]))

        return df


if __name__ == "__main__":
    print(SchemaElementsFrequency(is_debug=True,
                             xdm_schema="learning",
                             collection_name_tag="supply_tag_20191025",
                             mongo_client=BaseMongoClient()).process())
