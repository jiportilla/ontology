#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import time

from base import BaseObject


class ParseAPI(BaseObject):
    """ API for Manifest based Parse Activities """

    def __init__(self,
                 manifest_name: str,
                 activity_name: str,
                 first: int=-1,
                 last: int=-1,
                 is_debug: bool = False):
        """
        Created:
            6-Mar-2019
            craig.trim@ibm.com
        Updated:
            17-Dec-2019
            xavier.verges@es.ibm.com
            *    The API now provides independent methods for parsing, flushing, indexing
                and getting the number of records to process
        """
        from dataingest import ParseManifestData
        BaseObject.__init__(self, __name__)
        self.is_debug = is_debug
        self.parser = ParseManifestData(manifest_name,
                                        activity_name,
                                        first, last,
                                        is_debug)

    def parse(self):

        start = time.time()
        _total_parsed = self.parser.process()

        if self.is_debug:
            self.logger.debug("\n".join([
                "Manifest Parse Complete",
                "\tTotal Requested: {}".format(self.parser.last - self.parser.first +1),
                "\tTotal Parsed: {}".format(_total_parsed),
                "\tTotal Time: {}".format(time.time() - start)]))

    def flush_target(self):
        return self.parser.flush_target()

    def get_sources(self):
        return self.parser.source_collections()

    def index_target(self):
        return self.parser.index_target()


def call_parse_api(manifest_name, activity_name, action, first, last):
    first = int(first) if first else -1
    last = int(last) if last else -1
    print(f"API Parameters "
          f"(manifest-name={manifest_name}, "
          f"activity-name={activity_name}, "
          f"action={action}, "
          f"first={first}, last={last})")

    if manifest_name.startswith("parse"):
        api = ParseAPI(manifest_name,
                       activity_name,
                       first,
                       last)
        return getattr(api, action)()
    else:
        raise ValueError("\n".join([
            "Unrecognized Manifest Name",
            "\tName: {}".format(manifest_name)]))
    pass

def main(manifest_name, activity_name, start_x, start_y, flush_records):
    def _flush_records() -> bool:
        if flush_records.lower().startswith("true"):
            return True
        return False

    flush = _flush_records()
    if flush:
        call_parse_api(manifest_name, activity_name, 'flush_target', '', '')
    call_parse_api(manifest_name, activity_name, 'parse', start_x, start_y)

if __name__ == "__main__":
    import plac

    plac.call(main)
