#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from datamongo.core.bp import CendantCollection
from datamongo.core.dmo import BaseMongoClient


class SlackEventCollection(BaseObject):
    """ Collection Wrapper over the MongoDB Collection for slack events
    """

    _records = None

    def __init__(self,
                 base_mongo_client: BaseMongoClient = None,
                 database_name: str = "cendant",
                 collection_name: str = "event_slack",
                 is_debug: bool = False):
        """
        Created:
            18-Jul-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/453
        """
        BaseObject.__init__(self, __name__)
        if not base_mongo_client:
            base_mongo_client = BaseMongoClient()

        self.is_debug = is_debug
        self.collection = CendantCollection(some_base_client=base_mongo_client,
                                            some_db_name=database_name,
                                            some_collection_name=collection_name)

    def save(self,
             d_event: dict):
        self.collection.save(d_event)

    def by_ts(self):
        pass
