#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from base.core.dmo import BaseObject
from databp.core.dmo import BluepagesEndpointQuery
from databp.core.dmo import BluepagesResultFormatter
from databp.core.dmo import BluepagesResultMapper
from databp.core.dto import BluepagesEndpointQueryAdapter
from datamongo import CendantCollection


class QueryBluepagesEndpoint(BaseObject):
    def __del__(self):
        self.timer.log()

    def __init__(self):
        """
        Created:
            28-Nov-2018
            craig.trim@ibm.com
        """
        BaseObject.__init__(self, __name__)
        self.bp_api_raw_collection = CendantCollection(some_db_name="cendant",
                                                       some_collection_name="bluepages_raw")
        self.bp_api_mapped_collection = CendantCollection(some_db_name="cendant",
                                                          some_collection_name="bluepages_mapped")

    @staticmethod
    def _not_exists(some_internet_address):
        return {
            "INTERNET": some_internet_address,
            "EXISTS": False}

    def _lookup_in_mongo(self,
                         some_internet_address: str,
                         use_logger: bool) -> dict:
        _result = self.bp_api_raw_collection.by_field("internet_address",
                                                      some_internet_address)

        if _result:
            if use_logger:
                self.logger.debug("\n".join([
                    "Retrieved from MongoDB",
                    "\t{0}".format(_result)
                ]))

            return _result

    def _lookup_in_endpoint(self,
                            some_internet_address: str,
                            persist: bool) -> dict:

        if "ibm" not in some_internet_address:
            return self._not_exists(some_internet_address)

        result = BluepagesEndpointQuery().by_internet_address(some_internet_address)

        if not result or len(result) < 50:  # employee not in directory
            result = self._not_exists(some_internet_address)

        else:  # format the results
            result = BluepagesResultFormatter(result).process()
            result["EXISTS"] = True

        if persist:
            self.bp_api_raw_collection.save(result,
                                            caller=__name__)

            self.logger.debug("\n".join([
                "Persisted to MongoDB",
                "\t{0}".format(some_internet_address)
            ]))

        return result

    def by_internet_address_bulk(self,
                                 email_addresses: list,
                                 use_logger=True) -> int:
        total_loaded = 0

        already_loaded = set(self.bp_api_raw_collection.email_address())
        remaining = [x for x in email_addresses if x not in already_loaded]

        if use_logger:
            self.logger.debug("\n".join([
                "Bulk Loader Analysis",
                "\ttotal-email-addresses: {0}".format(len(email_addresses)),
                "\talready-loaded: {0}".format(len(already_loaded)),
                "\tremaining: {0}".format(len(remaining))
            ]))

        records = []
        for email_address in remaining:
            records.append(self._lookup_in_endpoint(email_address,
                                                    persist=False))
            if len(records) >= 500:
                self.bp_api_raw_collection.insert_many(records)
                records = []

            elif len(records) % 100 == 0:
                self.logger.debug("\n".join([
                    "\n\nRecord Lookup Total: {0}\n\n".format(len(records))
                ]))

        return total_loaded

    def by_internet_address(self,
                            some_internet_address,
                            use_cache=True,
                            filter_empty=False,
                            use_logger=True):
        BluepagesEndpointQueryAdapter.validate_internet_address(some_internet_address)

        if "ibm" not in some_internet_address:
            return self._not_exists(some_internet_address)

        def _lookup():
            _lookup_result = self._lookup_in_mongo(some_internet_address,
                                                   use_logger)
            if _lookup_result:
                return _lookup_result
            return self._lookup_in_endpoint(some_internet_address,
                                            persist=True)

        mongo_result = _lookup()
        mongo_result = BluepagesResultMapper(mongo_result).process()

        self.bp_api_mapped_collection.delete_by_email_address(mongo_result["email_address"],
                                                              use_logger=use_logger)

        return self.bp_api_mapped_collection.save(mongo_result,
                                                  caller=__name__)
