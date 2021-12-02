#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import time

from base import BaseObject
from cendalytics.tfidf.core.svc import ReadInversionLibrary


class VectorSpaceInversionRead(BaseObject):
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

    def read(self,
             library_name: str or None) -> ReadInversionLibrary:
        """
        Purpose:
            -   Read the Inversion Library
            -   Given a term (e.g., a Skill) this function will return
                results (e.g., Serial Numbers) for which this term is highly discriminating
        :param library_name:
            str     the library to read from
            None    the API will find the latest library
        :return:
            an instantiated ReadInversionLibrary
        """
        start = time.time()

        if not library_name or library_name.lower() == "latest":
            library_name = self._parent.list().process(library_type="inversion",
                                                       latest_only=True)

        reader = ReadInversionLibrary(library_name=library_name)

        if self._is_debug:
            end = round(time.time() - start, 2)
            self.logger.debug(f"Instantiated Inversion Reader ("
                              f"library={library_name}, "
                              f"time={end}s)")
        return reader
