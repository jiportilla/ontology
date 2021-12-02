#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from base import MandatoryParamError


class DeleteRecord(BaseObject):
    """ API for Deleting a Record (or series of Records) from MongoDB   """

    def __init__(self, some_collection):
        """
        Created:
            28-Nov-2018
            craig.trim@ibm.com
        Updated:
            15-Mar-2019
            craig.trim@ibm.com
            *   add strict typing
        """
        BaseObject.__init__(self, __name__)
        if not some_collection:
            raise MandatoryParamError("Collection")

        self.collection = some_collection

    def all(self,
            is_debug: bool = False) -> int:
        """
        :return:
            the total records delete
        """
        records_deleted = self.collection.delete_many({}).deleted_count
        if is_debug:
            self.logger.debug("\n".join([
                "Deleted Records (all)",
                "\ttotal: {0}".format(records_deleted),
                "\tcollection: {0}".format(self.collection.name)
            ]))

        return records_deleted

    def by_id(self,
              some_id: str,
              is_debug: bool = False) -> int:
        """
        :param some_id:
            the ID of the dialog structure to delete
        :param is_debug:
        :return:
            the total records delete
        """
        query = {"_id": some_id}
        records_deleted = self.collection.delete_many(
            query).deleted_count

        if is_debug:
            self.logger.debug("\n".join([
                "Deleted Records (by id)",
                "\tId: {0}".format(some_id),
                "\tTotal: {0}".format(records_deleted),
                "\tCollection: {0}".format(self.collection.name)]))

        return records_deleted

    def by_query(self,
                 some_query: dict,
                 is_debug: bool = False) -> int:
        """
        :param some_query:
            the query pattern to delete
        :param is_debug:
        :return:
            the total records delete
        """
        records_deleted = self.collection.delete_many(
            some_query).deleted_count

        if is_debug:
            self.logger.debug("\n".join([
                "Deleted Records (by id)",
                "\tQuery: {0}".format(some_query),
                "\tTotal: {0}".format(records_deleted),
                "\tCollection: {0}".format(self.collection.name)]))

        return records_deleted

    def by_ts(self, tts,
              is_debug: bool = False) -> int:
        """
        :param tts:
            the timestamp
        :param is_debug:
        :return:
            the total records delete
        """

        query = {"ts": tts}
        records_deleted = self.collection.delete_many(
            query).deleted_count

        if is_debug:
            self.logger.debug("\n".join([
                "Deleted Records (by ts)",
                "\tttts: {0}".format(tts),
                "\ttotal: {0}".format(records_deleted),
                "\tcollection: {0}".format(self.collection.name)
            ]))

        return records_deleted
