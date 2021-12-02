#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import os

from tabulate import tabulate

from dataingest import ManifestActivityFinder

IS_DEBUG = False


def manifest(manifest_name: str,
             activity_name: str) -> dict:
    return ManifestActivityFinder(some_manifest_name=manifest_name,
                                  some_manifest_activity=activity_name).process()


def xdm_schema(d_manifest) -> str:
    return d_manifest["dimensions"]


def call_dimensions_api(manifest_name, activity_name, action, first, last):
    from datamongo.core.dmo import BaseMongoClient
    from cendantdim.batch.svc import ProcessMultipleRecords
    from dataingest.core.dmo import ManifestConnectorForMongo

    first = int(first) if first else -1
    last = int(last) if last else -1
    print(f"API Parameters "
          f"(manifest-name={manifest_name}, "
          f"activity-name={activity_name}, "
          f"action={action}, "
          f"first={first}, last={last})")

    mongo_client = BaseMongoClient()
    d_manifest = manifest(manifest_name, activity_name)

    def collection_name(manifest_key: str) -> str:
        return ManifestConnectorForMongo(d_manifest[manifest_key], mongo_client).collection_name()

    api = ProcessMultipleRecords(d_manifest=None,
                                 xdm_schema=xdm_schema(d_manifest),
                                 collection_name_tag=collection_name('source'),
                                 collection_name_xdm=collection_name('target'),
                                 mongo_client=mongo_client,
                                 is_debug=IS_DEBUG)
    if action == 'parse':
        api.by_record_paging(start_record=first,
                             end_record=last,
                             flush_records=False)
    else:
        return getattr(api, action)()


def main(param1,  # manifest-name
         param2,  # activity-name
         param3,  # action
         param4,  # keyfield-value OR start-record
         param5,  # None or end-record
         param6):  # None or flush-records

    print("\n".join([
        "Parameter List",
        f"\tParam-1 (Manifest Name): {param1}",
        f"\tParam-2 (Activity Name): {param2}",
        f"\tParam-3 (Action): {param3}",
        f"\tParam-4 (Keyfield/Start Record): {param4}",
        f"\tParam-5 (Optional/End Record): {param5}",
        f"\tParam-6 (Optional/Flush Records): {param6}"]))

    d_manifest = manifest(param1, param2)

    def _is_multi_action() -> bool:
        return str(param3).lower().strip().startswith("multi")

    def _flush_records() -> bool:
        return str(param6).lower().startswith("t") or str(param6) == "1"

    def _start_record() -> int:
        return int(param4)

    def _end_record() -> int:
        return int(param5)

    def _collection_name_tag() -> str:
        return os.environ['SOURCE_COLLECTION']

    def _collection_name_xdm() -> str:
        return os.environ['TARGET_COLLECTION']

    def _host():
        return "wftag"

    from cendantdim.batch.bp import DimensionsBatchAPI

    api = DimensionsBatchAPI(is_debug=IS_DEBUG)

    if _is_multi_action():
        api.multiple_records(d_manifest=d_manifest,
                             start_record=_start_record(),
                             end_record=_end_record(),
                             xdm_schema=xdm_schema(d_manifest),
                             flush_records=_flush_records(),
                             collection_name_tag=_collection_name_tag(),
                             collection_name_xdm=_collection_name_xdm())
    else:
        df_result = api.single_record(d_manifest=d_manifest,
                                      mongo_host=_host(),
                                      persist_result=False,
                                      key_field=str(param4),
                                      xdm_schema=xdm_schema(d_manifest),
                                      collection_name_tag=_collection_name_tag(),
                                      collection_name_xdm=_collection_name_xdm())

        print(tabulate(df_result,
                       headers='keys',
                       tablefmt='psql'))


if __name__ == "__main__":
    import plac

    plac.call(main)
