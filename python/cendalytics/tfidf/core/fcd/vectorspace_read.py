#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import time

from base import BaseObject
from cendalytics.tfidf.core.svc import ReadCollectionVectorSpace


class VectorSpaceRead(BaseObject):
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

    def read(self,
             library_name: str or None) -> ReadCollectionVectorSpace:
        """
        Purpose:
        Traceability:
        :param library_name:
            str     the library to read from
            None    the API will find the latest library
        :return:
            an instantiated ReadCollectionVectorSpace
            Sample Output:
                +----+--------+------------+
                |    |   Rank | Tag        |
                |----+--------+------------|
                |  0 |      1 | windows nt |
                |  1 |      2 | rfs        |
                |  2 |      3 | microsoft  |
                +----+--------+------------+
        """
        start = time.time()

        if not library_name or library_name.lower() == "latest":
            library_name = self._parent.list().process(library_type="vectorspace",
                                                       latest_only=True)
            if not library_name:
                raise ValueError

        reader = ReadCollectionVectorSpace(is_debug=self._is_debug,
                                           library_name=library_name)

        if self._is_debug:
            self.logger.debug(f"Instantiated Reader ("
                              f"library={library_name}, "
                              f"time={round(time.time() - start, 2)}s)")
        return reader
