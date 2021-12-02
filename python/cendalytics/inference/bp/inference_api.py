#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject


class InferenceAPI(BaseObject):
    """ Inference API """

    def __init__(self,
                 is_debug: bool = False):
        """
        Created:
            6-Nov-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1230
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug

    def skills(self):
        pass

    def query(self):
        pass

    def inversion(self):
        pass


