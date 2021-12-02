#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from cendalytics.tfidf.core.bp import VectorSpaceAPI
from datamongo import BaseMongoClient
from datamongo import CendantCollection


class CreateAndInvertAll(BaseObject):
    """
    Purpose:
        Given a Collection,
        1.  Create a Vector Space for the entire collection
            and invert this Vector Space
        2.  For each division represented in the Vector Space, create a Vector Space
            and invert this Vector Space
    """

    def __init__(self,
                 collection_name: str):
        BaseObject.__init__(self, __name__)
        self._collection_name = collection_name

    def process(self):
        vectorspace_api = VectorSpaceAPI(is_debug=False)

        mongo_client = BaseMongoClient()
        collection = CendantCollection(some_base_client=mongo_client,
                                       some_collection_name=self._collection_name)

        for division in collection.distinct("div_field"):
            vs_fpath = vectorspace_api.tfidf().create(division=division,
                                                      mongo_client=mongo_client,
                                                      collection_name=self._collection_name).process()
            print(f"VS Library Path: {vs_fpath}")

            vs_library_name = vs_fpath.split('/')[-1]
            print(f"VS Library Name: {vs_library_name}")

            inversion_fpath = vectorspace_api.inversion().create(vs_library_name).process(top_n=3)
            print(f"Inversion Library Path: {inversion_fpath}")

            inversion_library_name = inversion_fpath.split('/')[-1]
            print(f"Inversion Library Name: {inversion_library_name}")


def main():
    COLLECTION_NAME = 'supply_tag_20191025'
    CreateAndInvertAll(collection_name=COLLECTION_NAME).process()


if __name__ == "__main__":
    main()
