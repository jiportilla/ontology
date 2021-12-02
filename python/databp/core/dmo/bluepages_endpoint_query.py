#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import requests

from base.core.dmo import BaseObject


class BluepagesEndpointQuery(BaseObject):
    def __del__(self):
        self.timer.log()

    def __init__(self):
        """
        Created:
            28-Nov-2018
            craig.trim@ibm.com
        """
        BaseObject.__init__(self, __name__)

    byInternetAddrUrl = "http://bluepages.ibm.com/BpHttpApisv3/wsapi?byInternetAddr={0}"

    def by_internet_address(self, some_internet_address):

        url = self.byInternetAddrUrl.format(some_internet_address)
        r = requests.get(url)

        return r.text
