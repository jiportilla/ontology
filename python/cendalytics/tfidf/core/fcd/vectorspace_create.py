#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import time

from base import BaseObject
from base import DataTypeError
from base import MandatoryParamError
from cendalytics.tfidf.core.svc import CreateCollectionVectorSpace
from datamongo import BaseMongoClient


class VectorSpaceCreate(BaseObject):
    """ VectorSpace Function Facade """

    def __init__(self,
                 parent,
                 is_debug: bool = False):
        """
        Created:
            6-Nov-2019
            craig.trim@ibm.com
        """
        BaseObject.__init__(self, __name__)
        self._parent = parent
        self._is_debug = is_debug

    @staticmethod
    def validate(collection_name: str,
                 mongo_client: BaseMongoClient,
                 division: str = None,
                 limit: int = 0) -> None:
        if not collection_name:
            raise MandatoryParamError("Collection Name")
        if 'tag' not in collection_name.lower():
            raise ValueError("Tag Collection Expected")
        if not mongo_client:
            raise MandatoryParamError("Mongo Client")
        if division and type(division) != str:
            raise DataTypeError("Division")
        if limit and type(limit) != int:
            raise DataTypeError("Limit")

    def create(self,
               collection_name: str,
               division: str or None,
               mongo_client: BaseMongoClient,
               limit: int = None) -> CreateCollectionVectorSpace:
        """
        Purpose:
            Generate a Vector Space via TF-IDF Metrics
        Traceability:
            https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1261#issuecomment-15732844
        :param collection_name:
            the name of the tag collection to build the vector space across
        :param division:
            the division to focus on
        :param mongo_client:
            an instantiated mongoDB connection instance
        :param limit:
            Optional    the number of records to return
        :return:
            an instantiated GenerateVectorSpace
            Sample Output:
                +-------+-----------+----------------+----------+---------+-----------+---------------------+-------------------------+----------------------+--------------+-------------+
                |       | Doc       |   DocsWithTerm |      IDF |      TF |     TFIDF | Term                |   TermFrequencyInCorpus |   TermFrequencyInDoc |   TermsInDoc |   TotalDocs |
                |-------+-----------+----------------+----------+---------+-----------+---------------------+-------------------------+----------------------+--------------+-------------|
                |     0 | 0697A5744 |            159 |  2.39904 | 0.06667 |   0.02779 | cloud service       |                     186 |                    1 |           15 |        1751 |
                |     1 | 0697A5744 |           1094 |  0.47035 | 0.06667 |   0.14174 | management          |                    2573 |                    1 |           15 |        1751 |
                |     2 | 0697A5744 |           2006 | -0.13596 | 0.06667 |  -0.49036 | agile               |                    2194 |                    1 |           15 |        1751 |
                |     3 | 0697A5744 |           2995 | -0.53676 | 0.06667 |  -0.1242  | ibm                 |                    5857 |                    1 |           15 |        1751 |
                |     4 | 0697A5744 |            513 |  1.22767 | 0.06667 |   0.0543  | data science        |                     745 |                    1 |           15 |        1751 |
                ...
                | 97480 | 04132K744 |            479 |  1.29624 | 0.01754 |   0.01353 | maintenance         |                     945 |                    1 |           57 |        1751 |
                +-------+-----------+----------------+----------+---------+-----------+---------------------+-------------------------+----------------------+--------------+-------------+
        """
        start = time.time()

        self.validate(collection_name=collection_name,
                      mongo_client=mongo_client,
                      division=division,
                      limit=limit)

        gen = CreateCollectionVectorSpace(limit=limit,
                                          division=division,
                                          is_debug=self._is_debug,
                                          mongo_client=mongo_client,
                                          collection_name=collection_name)
        if self._is_debug:
            self.logger.debug(f"Instantiated Generator ("
                              f"collection={collection_name}, "
                              f"time={round(time.time() - start, 2)}s)")
        return gen
