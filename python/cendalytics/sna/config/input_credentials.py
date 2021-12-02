#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject


class InputCredentials(BaseObject):
    """ Set Input Credentials """

    def __init__(self):
        """
        Created:
            - 18-July-2019
            - abhbasu3@in.ibm.com
        """
        BaseObject.__init__(self, __name__)

    intranet_id = None
    intranet_pass = None

    @staticmethod
    def set_config_details(intra_id, intra_pass):
        InputCredentials.intranet_id = intra_id
        InputCredentials.intranet_pass = intra_pass

    @staticmethod
    def getintranet():
        return InputCredentials.intranet_id, InputCredentials.intranet_pass
