#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pymongo
from pymongo.collection import Collection
from pymongo.errors import OperationFailure

from base import BaseObject


class CreateFieldIndex(BaseObject):
    """ Creates a basic Field Index on a MongoDB Collection
    """

    _records = None

    def __init__(self,
                 collection: Collection,
                 is_debug: bool = False):
        """
        Created:
            5-Dec-2019
            craig.trim@ibm.com
            *   based on 'create-text-index'
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1515#issuecomment-16439788
        Updated:
            6-Dec-2019
            abhbasu3
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1529
        Updated:
            17-Dec-2019
            xavier.verges@es.ibm.com
            *   Allow options. Do not force foreground indexing
        """
        BaseObject.__init__(self, __name__)

        self._is_debug = is_debug
        self._collection = collection

    def process(self,
                field_name: str,
                sort_order: int = pymongo.ASCENDING,
                default_language: str = 'english',
                index_options = None) -> None:

        def _field_name():
            return field_name.replace('.', '_')

        try:
            if index_options is None:
                index_options = {}

            field_index = [(field_name, sort_order)]
            self._collection.create_index(field_index,
                                          name=_field_name(),
                                          default_language=default_language,
                                          **index_options)

        except OperationFailure as err:
            self.logger.error('\n'.join([
                f"Index Creation Error",
                f"\tField Name: {field_name}",
                f"\tSort Order: {sort_order}",
                f"\tDefault Language: {default_language}",
                f"\tCollection: {self._collection.name}"]))
            self.logger.exception(err)
            # raise ValueError
