#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from base import FileIO


class CollectionNameWeight(BaseObject):

    def __init__(self,
                 is_debug: bool = False):
        """
        Created:
            8-May-2019
            craig.trim@ibm.com
            *   refactored out of 'generate-dataframe-weights'
        """
        BaseObject.__init__(self, __name__)
        self.is_debug = is_debug
        self.collection_weights = self._collection_weight_config()

    @staticmethod
    def _collection_weight_config():
        d = {}

        def _doc() -> dict:
            return FileIO.file_to_yaml_by_relative_path(
                "resources/config/weights/collection_weights.yml")

        for item in _doc()["collections"]:
            d[item["collection"]] = item["weight"]

        return d

    def process(self,
                collection_name: str) -> float:
        if collection_name in self.collection_weights:
            return self.collection_weights[collection_name]

        return 1.0
