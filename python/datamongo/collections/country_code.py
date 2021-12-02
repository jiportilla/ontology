#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from datamongo.core.bp import CendantCollection
from datamongo.core.dmo import BaseMongoClient


class CountryCode(BaseObject):
    """ Collection Wrapper over MongoDB Collection for "ingest_country_code"   """

    _records = None

    def __init__(self,
                 some_base_client=None,
                 is_debug: bool = False):
        """
        Created:
            14-May-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/254
        """
        BaseObject.__init__(self, __name__)
        if not some_base_client:
            some_base_client = BaseMongoClient()

        self.is_debug = is_debug
        self.collection = CendantCollection(some_base_client=some_base_client,
                                            some_db_name="cendant",
                                            some_collection_name="ingest_country_code")

    def all(self,
            limit: int = None):
        if not self._records:
            def _records():
                if not limit:
                    return self.collection.all()
                return self.collection.skip_and_limit(0, limit)

            self._records = _records()
            self.logger.debug("\n".join([
                "Retrieved Records",
                "\tTotal: {}".format(len(self._records))]))

        return self._records
