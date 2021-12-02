#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from pymongo import TEXT
from pymongo.collection import Collection
from pymongo.errors import OperationFailure

from base import BaseObject


class CreateTextIndex(BaseObject):
    """ Creates a Text Index on a MongoDB Collection
    """

    _records = None

    def __init__(self,
                 collection: Collection,
                 is_debug: bool = False):
        """
        Created:
            24-Jun-2019
            craig.trim@ibm.com
            *   loosely based on 'query-elasticsearch'
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/371
        Updated:
            15-Oct-2019
            craig.trim@ibm.com
            *   pass in field name for indexing and update logging
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1122
        """
        BaseObject.__init__(self, __name__)

        self.is_debug = is_debug
        self.collection = collection

    def process(self,
                field_name: str = 'fields.normalized',
                index_options = None) -> None:

        try:

            if index_options is None:
                index_options = {}

            self.collection.create_index(
                [(field_name, TEXT)],
                default_language='english',
                **index_options)

            if self.is_debug:
                self.logger.debug("\n".join([
                    "Text Index Creation Successful"]))

        except OperationFailure as err:
            self.logger.error('\n'.join([
                f"Text Index Creation Error: {err}",
                f"Collection: {self.collection.name}",
                f"Field Name: {field_name}"]))
            self.logger.exception(err)
            raise ValueError


def main(collection_name, field_name):
    from datamongo import CendantCollection
    collection = CendantCollection(some_collection_name=collection_name).collection
    CreateTextIndex(collection=collection, is_debug=True).process(field_name=field_name)


if __name__ == "__main__":
    import plac

    plac.call(main)
