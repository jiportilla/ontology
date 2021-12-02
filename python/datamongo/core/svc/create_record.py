#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import pprint

from pymongo.errors import DuplicateKeyError

from base import BaseObject
from base import MandatoryParamError


class CreateRecord(BaseObject):
    """ API for creating a Record (or series of Records) from MongoDB   """

    def __init__(self, some_collection):
        """
        Created:
            28-Nov-2018
            craig.trim@ibm.com
        """
        BaseObject.__init__(self, __name__)
        if not some_collection:
            raise MandatoryParamError("Collection")

        self.collection = some_collection

    def _delete_by_id_and_ts(self, some_obj, is_debug: bool):
        from datamongo.core.svc import DeleteRecord

        DeleteRecord(self.collection).by_id(some_id=some_obj["_id"],
                                            is_debug=is_debug)

        DeleteRecord(self.collection).by_ts(tts=some_obj["ts"],
                                            is_debug=is_debug)

    def _insert_many(self, some_list, ignore_duplicate_key, is_debug: bool, overwrite=False):
        try:

            if overwrite:
                for item in [x for x in some_list if "_id" in x]:
                    self._delete_by_id_and_ts(item, is_debug)

            return self.collection.insert_many(some_list)

        except DuplicateKeyError:

            if not ignore_duplicate_key:
                raise ValueError(
                    "Attempted to Insert Duplicate Item from List")

            if ignore_duplicate_key and is_debug:
                self.logger.warning("\n".join([
                    "Attempted to Insert Duplicate Item from List:",
                    "\tCollection: {0}".format(self.collection.name),
                    pprint.pformat(some_list, indent=4)
                ]))

    def _insert_one(self, some_obj, ignore_duplicate_key, is_debug:bool, overwrite=False):
        try:

            if overwrite and "_id" in some_obj:
                self._delete_by_id_and_ts(some_obj, is_debug)

            event_id = self.collection.insert_one(some_obj).inserted_id

            if is_debug:
                self.logger.debug("\n".join([
                    "Inserted Object",
                    "\tID = {0}".format(event_id),
                    "\tCollection: {0}".format(self.collection.name)
                ]))

            if "_id" not in some_obj:
                some_obj["_id"] = event_id

            return some_obj

        except DuplicateKeyError:

            if not ignore_duplicate_key:
                raise ValueError(
                    "Attempted to Insert Duplicate Object (_id = {0})".format(
                        some_obj["_id"]))

            if ignore_duplicate_key and is_debug:
                self.logger.warning("\n".join([
                    "Attempted to Insert Duplicate Object:",
                    "\tID = {0}".format(some_obj["_id"]),
                    "\tCollection: {0}".format(self.collection.name)
                ]))

    def save(self, some_obj, some_caller=None, ignore_duplicate_key=False, is_debug=False, overwrite=False):
        """
        :param some_obj:
            the object to persist
            -   this component will supply a unique _id field if one does not exist
        :param some_caller:
            the class that invoked this persistence method (for service traceability)
        :param ignore_duplicate_key:
            if True     log a duplicate key exception
            if False    throw an exception for the user to deal with
        :param is_debug:
            if True     perform logging
            if False    (default) do not log activity
        :return:
            the persisted event
            guaranteed to include a mongoDB '_id' field
        """

        if "caller" not in some_obj and some_caller:
            some_obj["caller"] = some_caller

        if "ts" not in some_obj:
            some_obj["ts"] = BaseObject.generate_tts(ensure_random=False)

        if is_debug:
            self.logger.debug("\n".join([
                "Attempting to Persist Object",
                pprint.pformat(some_obj, indent=4)
            ]))

        if isinstance(some_obj, (list, set)):
            return self._insert_many(some_obj,
                                     ignore_duplicate_key,
                                     is_debug,
                                     overwrite)

        return self._insert_one(some_obj,
                                ignore_duplicate_key,
                                is_debug,
                                overwrite)
