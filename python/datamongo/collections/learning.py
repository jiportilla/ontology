#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from datamongo.core.bp import CendantCollection
from datamongo.core.dmo import BaseMongoClient


class Learning(BaseObject):
    """ Collection Wrapper over MongoDB Collection for "learningactivity   """

    _records = None

    def __init__(self,
                 some_base_client=None):
        """
        Created:
            25-Apr-2019
            craig.trim@ibm.com
        """
        BaseObject.__init__(self, __name__)
        if not some_base_client:
            some_base_client = BaseMongoClient()

        self.collection = CendantCollection(some_base_client=some_base_client,
                                            some_db_name="cendant",
                                            some_collection_name="ingest_learning_activities")

    def all(self):
        if not self._records:
            self._records = self.collection.all()
            self.logger.debug("\n".join([
                "Retrieved Records (total={})".format(
                    len(self._records))]))

        return self._records

    def available(self):
        self.collection.find_by_query({})
