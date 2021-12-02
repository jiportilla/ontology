#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import random
import time

from base import BaseObject


class IngestAPI(BaseObject):
    """ API for Manifest based Ingestion Activities """

    def __init__(self):
        """
        Created:
            6-Mar-2019
            craig.trim@ibm.com
        Updated:
            26-June-2019
            abhbasu3@in.ibm.com
            *   added db2 auto ingestion
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/370
        """
        BaseObject.__init__(self, __name__)

    @staticmethod
    def ingest(manifest_name: str,
               manifest_activity: str,
               flush_records: bool,
               random_delay: str):
        from dataingest import IngestManifestData

        try:
            delay = random.randrange(int(random_delay))
        except:                                             # noqa: E722
            delay = 0

        ingest_api = IngestManifestData(manifest_name, manifest_activity)
        if delay:
            ingest_api.logger.debug(f'Random delay of {delay} seconds before reaching db2')
            time.sleep(delay)
        ingest_api.process(flush_target_records=flush_records)


def main(manifest_name, manifest_activity, flush_records, random_delay='0'):
    flush_records = flush_records.lower() == "true"

    print(f"API Parameters "
          f"(manifest-name={manifest_name}, "
          f"activity-name={manifest_activity}, "
          f"flush-records={flush_records})")

    if manifest_name.startswith("ingest"):
        IngestAPI.ingest(manifest_name,
                         manifest_activity,
                         flush_records,
                         random_delay)
    else:
        raise ValueError(f"Unrecognized Manifest: {manifest_name}")


if __name__ == "__main__":
    import plac

    plac.call(main)
