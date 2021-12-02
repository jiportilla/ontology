#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject


class CendantDB2(BaseObject):
    """ Create a DB2 connection """

    def __init__(self,
                 is_debug: bool = False):
        """
        Created:
            9-Oct-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1080
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug

    def cendant(self):
        """

        :return:
        """
        pass

    def wft(self):
        """

        :return:
        """
        pass

    def wftdev(self):
        """

        :return:
        """
        pass


    def lur(self):
        """

        :return:
        """
        pass