#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from base.core.dmo import BaseObject
from databp.core.dmo import BluepagesResultMapper
from datamongo import CendantCollection


class MapRawEvents(BaseObject):
    """ transforms all the raw events into mapped events """

    def __del__(self):
        self.timer.log()

    def __init__(self):
        """
        Created:
            30-Nov-2018
            craig.trim@ibm.com
        """
        BaseObject.__init__(self, __name__)
        self.bp_api_raw_collection = CendantCollection(some_db_name="cendant",
                                                       some_collection_name="bluepages_raw")
        self.bp_api_mapped_collection = CendantCollection(some_db_name="cendant",
                                                          some_collection_name="bluepages_mapped")

    def process(self):
        """
        :return:
        """

        self.bp_api_mapped_collection.delete()
        raw_events = self.bp_api_raw_collection.all()

        ctr = 0
        total = len(raw_events)

        records = []
        for raw_event in raw_events:

            ctr += 1
            records.append(BluepagesResultMapper(raw_event).process())

            if len(records) > 1000:
                self.bp_api_mapped_collection.insert_many(records)
                records = []

            elif ctr % 100 == 0:
                self.logger.debug("\n".join([
                    "Processing Event: {0} - {1}".format(ctr,
                                                         total)
                ]))
