#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from random import randint

from base import BaseObject
from datamongo.core.bp import CendantCollection
from datamongo.core.dmo import BaseMongoClient


class Employees(BaseObject):
    """ Collection Wrapper over MongoDB Collection for "_employees   """

    _records = None

    def __init__(self,
                 some_base_client=None):
        """
        Created:
            15-Apr-2019
            craig.trim@ibm.com
        """
        BaseObject.__init__(self, __name__)
        if not some_base_client:
            some_base_client = BaseMongoClient()

        self.collection = CendantCollection(some_base_client=some_base_client,
                                            some_db_name="cendant",
                                            some_collection_name="ingest_employees")

    def all(self):
        if not self._records:
            self._records = self.collection.all()
            self.logger.debug("\n".join([
                "Retrieved Records",
                "\tTotal: {}".format(len(self._records))]))

        return self._records

    def region_by_cnum(self):
        d_index = {}

        for record in self.all():
            cnum = [x["value"] for x in record["fields"] if x["name"] == "serial_number_lookup"][0]
            value = [x["value"] for x in record["fields"] if x["name"] == "region"][0]
            if value:
                d_index[cnum] = value

        return d_index
