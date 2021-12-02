#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import time

from datamongo import CendantTag
from datamongo import TransformCendantRecords
from datamongo.core.dmo import BaseMongoClient

IS_DEBUG = True
SERVER_ALIAS = "CLOUD"
TAGS_AS_CSV = False
COLLECTION_NAME = "supply_tag_20200116"

# Uncomment to retrieve all records:
QUERY = {}

# Uncomment to retrieve certain recoreds:
# QUERY = {"fields.name": "badge_name"}

# Uncomment to retrieve all records
QUERY_LIMIT = None


# Uncomment to limit records
# QUERY_LIMIT = 500


def main():
    start = time.time()
    file_name = f"{COLLECTION_NAME}-Allw"

    host = BaseMongoClient(is_debug=IS_DEBUG,
                           server_alias=SERVER_ALIAS)

    cendant_tag = CendantTag(is_debug=IS_DEBUG,
                             mongo_client=host,
                             collection_name=COLLECTION_NAME)

    records = cendant_tag.collection.find_by_query(some_query=QUERY,
                                                   limit=QUERY_LIMIT)
    print('\n'.join([
        f"Located Records (total={len(records)})",
        f"\tTotal Time: {round(time.time() - start, 1)}s"]))

    if not records:
        raise ValueError("No Records")

    TransformCendantRecords.to_file(records=records,
                                    include_text=IS_DEBUG,
                                    tags_as_csv=TAGS_AS_CSV,
                                    file_name=file_name)


if __name__ == "__main__":
    main()
