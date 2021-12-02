#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from base import MandatoryParamError


class GraphDataExtractor(BaseObject):
    """ """

    def __init__(self,
                 some_mongo_collections: dict,
                 some_mongo_source: dict):
        """
        Created:
            15-Mar-2019
            craig.trim@ibm.com
            *   refactored out of 'load-neo-from-manifest-2'
        :param some_mongo_collections:
            the name of the manifest
        :param some_mongo_source:
            the name of the activity within the manifest
        """
        BaseObject.__init__(self, __name__)
        if not some_mongo_collections:
            raise MandatoryParamError("Mongo Collections")
        if not some_mongo_source:
            raise MandatoryParamError("Mongo Source")

        self.mongo_source = some_mongo_source
        self.mongo_collections = some_mongo_collections

    def _source_records(self,
                        collection: str) -> list:

        def _limit():
            if "limit" not in self.mongo_source["description"]:
                return None
            try:
                return int(self.mongo_source["description"]["limit"])
            except ValueError:
                return None

        return self.mongo_collections[collection].all(_limit())

    def process(self) -> list:
        """ retrieve all source records
            apply a limit if applicable """

        collection = self.mongo_source["description"]["collection"]
        source_records = self._source_records(collection)

        self.logger.debug("\n".join([
            "Retrieved MongoDB Records",
            "\tcollection-name: {}".format(collection),
            "\ttotal-records: {}".format(len(source_records))
        ]))

        return source_records
