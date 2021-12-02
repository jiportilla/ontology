#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import time

from base import BaseObject
from cendalytics.tfidf.core.svc import InvertCollectionVectorSpace


class VectorSpaceInversionCreate(BaseObject):
    """ VectorSpace Function Facade """

    def __init__(self,
                 parent,  # maintain context to parent
                 is_debug: bool = False):
        """
        Created:
            6-Nov-2019
            craig.trim@ibm.com
        """
        BaseObject.__init__(self, __name__)
        self._parent = parent
        self._is_debug = is_debug

    def create(self,
               library_name: str) -> InvertCollectionVectorSpace:
        """
        Purpose:
            Invert the Vector Space by Term
        Implementation:
            -   The output is keyed by Term
                and each term is a list of values
                where each value is a key field in the underlying collection
            -   For example, in a vector space across supply_tag_<date>
                each term has a list of serial numbers
                and those serial numbers represent individuals for whom that term
                    is a discriminating skill
        Sample Output:
            +------+------------+-------------------------------------------+
            |      | KeyField   | Tag                                       |
            |------+------------+-------------------------------------------|
            |    0 | 0697A5744  | cloud service                             |
            |    1 | 0697A5744  | data science                              |
            |    2 | 0697A5744  | solution design                           |
            |    3 | 05817Q744  | kubernetes                                |
            |    4 | 05817Q744  | bachelor of engineering                   |
            |    5 | 05817Q744  | developer                                 |
            ...
            | 1328 | 249045760  | electrical engineering                    |
            +------+------------+-------------------------------------------+
        :param library_name:
            the name of the vector space library to invert
        :return:
            an instantiated CreateInvertedVectorSpace
        """
        start = time.time()
        inverter = InvertCollectionVectorSpace(library_name=library_name,
                                               is_debug=self._is_debug)

        if self._is_debug:
            self.logger.debug(f"Instantiated Inverter ("
                         f"library={library_name}, "
                         f"time={round(time.time() - start, 2)}s)")
        return inverter
