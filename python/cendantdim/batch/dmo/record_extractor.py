#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from base import MandatoryParamError
from datamongo import CendantCollection


class RecordExtractor(BaseObject):
    """ Perform incremental record extraction for dimensional computation

        this is only useful when we want to perform dimensionality
        on successive subsets of the data """

    def __init__(self,
                 some_manifest: dict,
                 total_records: int,
                 is_debug=False):
        """
        Created:
            28-Mar-2019
            craig.trim@ibm.com
            *   refactored out of 'transform-parsed-data'
        :param some_manifest:
            the manifest definition driving this component
        :param total_records:
            (optional)  the total records to process
        """
        BaseObject.__init__(self, __name__)
        if not some_manifest:
            raise MandatoryParamError("Manifest")

        self.is_debug = is_debug
        self.d_manifest = some_manifest
        self.total_records = total_records

    def _source_collection(self) -> CendantCollection:
        return CendantCollection(self.d_manifest["source"]["database"],
                                 self.d_manifest["source"]["collection"])

    def _target_collection(self) -> CendantCollection:
        return CendantCollection(self.d_manifest["target"]["database"],
                                 self.d_manifest["target"]["collection"])

    def _next_record_batch(self,
                           source_records: list,
                           target_records: list):
        """
        this function will ensure the next batch of records
        contains records that do not yet exist in the target collection
        :return:
            the next batch of records
            the size is determined by the value for 'total-records'
        """

        # load the keyfields for both source and target collections
        source_key_fields = set([x["key_field"] for x in source_records])
        target_key_fields = set([x["key_field"] for x in target_records])

        # find the untreated records
        diff = source_key_fields.difference(target_key_fields)

        # limit the size of the 'diff' set using 'total-records'
        if len(diff) > self.total_records:
            diff = list(diff)[:self.total_records]

        # return all the source records that are in the 'diff' set
        return [x for x in source_records
                if x["key_field"] in diff]

    def process(self) -> list:
        """
        :return:
            the next set of records to process
        """

        source_collection = self._source_collection()
        source_records = source_collection.all()

        if not self.total_records:
            if self.is_debug:
                self.logger.debug("\n".join([
                    "Source Records Located",
                    "\tTotal Source Records: {}".format(len(source_records))]))

            return source_records

        target_collection = self._target_collection()
        target_records = target_collection.all()

        records = self._next_record_batch(source_records,
                                          target_records)

        if self.is_debug:
            self.logger.debug("\n".join([
                "Records Located",
                "\tTotal Source Records: {}".format(len(source_records)),
                "\tTotal Target Records: {}".format(len(target_records)),
                "\tTotal Current Records: {}".format(len(records))]))

        return records
