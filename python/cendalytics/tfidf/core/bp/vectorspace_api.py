#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from cendalytics.tfidf.core.fcd import VectorSpaceCreate
from cendalytics.tfidf.core.fcd import VectorSpaceInversionCreate
from cendalytics.tfidf.core.fcd import VectorSpaceInversionRead
from cendalytics.tfidf.core.fcd import VectorSpaceRead
from cendalytics.tfidf.core.svc import CreateCollectionVectorSpace
from cendalytics.tfidf.core.svc import InvertCollectionVectorSpace
from cendalytics.tfidf.core.svc import ListVectorSpaceLibraries
from cendalytics.tfidf.core.svc import ReadCollectionVectorSpace
from cendalytics.tfidf.core.svc import ReadInversionLibrary
from datamongo import BaseMongoClient


class VectorSpaceAPI(BaseObject):
    """ Vector Space API """

    def __init__(self,
                 is_debug: bool = False):
        """
        Created:
            5-Nov-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1261#issuecomment-15732844
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug

    def list(self) -> ListVectorSpaceLibraries:
        """
        Purpose:
            List all the libraries the API is aware of
                and how to connect to them
        :return:
            an instantiated ListVectorSpaceLibraries
        """

        return ListVectorSpaceLibraries(is_debug=self._is_debug)

    def inversion(self):
        """ Functions related to CRUD operations on the Inverted VectorSpace """

        class Facade:
            @staticmethod
            def create(library_name: str) -> InvertCollectionVectorSpace:
                """ Create an Inverted VectorSpace """
                _o = VectorSpaceInversionCreate(self, is_debug=self._is_debug)
                return _o.create(library_name)

            @staticmethod
            def read(library_name: str) -> ReadInversionLibrary:
                """ Read an Inverted VectorSpace """
                _o = VectorSpaceInversionRead(self, is_debug=self._is_debug)
                return _o.read(library_name)

        return Facade()

    def tfidf(self):
        """ Functions related to CRUD operations on the VectorSpace """

        class Facade:
            @staticmethod
            def create(collection_name: str,
                       division: str or None,
                       mongo_client: BaseMongoClient,
                       limit: int = None) -> CreateCollectionVectorSpace:
                """ Create a VectorSpace """
                _o = VectorSpaceCreate(self, is_debug=self._is_debug)
                return _o.create(limit=limit,
                                 division=division,
                                 mongo_client=mongo_client,
                                 collection_name=collection_name)

            @staticmethod
            def read(library_name: str) -> ReadCollectionVectorSpace:
                """ Read a VectorSpace """
                _o = VectorSpaceRead(self, is_debug=self._is_debug)
                return _o.read(library_name)

        return Facade()
