#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import time

from base import BaseObject

IS_DEBUG = True


class AssembleAPI(BaseObject):
    """ API for Manifest based Merging Activities """

    def __init__(self,
                 manifest_name: str,
                 activity_name: str,
                 single_collection: str='',
                 first: int=-1,
                 last: int=-1):
        """
        Created:
            12-Mar-2019
            craig.trim@ibm.com
        Updated:
            10-May-2019
            craig.trim@ibm.com
            *   updated logging
        """
        from dataingest import AssembleManifestData
        BaseObject.__init__(self, __name__)
        self.assembler = AssembleManifestData(manifest_name,
                                              activity_name,
                                              single_collection,
                                              first,
                                              last,
                                              is_debug=IS_DEBUG)

    def assemble(self):

        start = time.time()
        self.assembler.process()
        total_time = time.time() - start
        self.logger.debug(f"Manifest Assembly Complete "
                          f"(time={total_time})")

    def flush_target(self):
        return self.assembler.flush_target()

    def get_sources(self):
        return self.assembler.source_collections()

    def index_target(self):
        return self.assembler.index_target()



def call_assemble_api(manifest_name, activity_name, action, single_collection, first, last):
    first = int(first) if first else -1
    last = int(last) if last else -1
    print(f"API Parameters "
          f"(manifest-name={manifest_name}, "
          f"activity-name={activity_name}, "
          f"action={action}, "
          f"single_collection={single_collection}, "
          f"first={first}, last={last})")

    if manifest_name.startswith("assemble"):
        api = AssembleAPI(manifest_name,
                          activity_name,
                          single_collection,
                          first,
                          last)
        return getattr(api, action)()
    else:
        raise ValueError(f"Unrecognized Manifest: {manifest_name}")


def main(manifest_name, activity_name):
    call_assemble_api(manifest_name, activity_name, 'assemble', '' , -1, -1)


if __name__ == "__main__":
    import plac

    plac.call(main)
