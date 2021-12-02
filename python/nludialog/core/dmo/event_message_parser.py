#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pprint

from slack import RTMClient
from slack import WebClient

from base import BaseObject


class EventMessageParser(BaseObject):
    """ Parse a Message Event """

    def __init__(self,
                 payload: dict):
        """
        Created:
            18-Jul-2019
            craig.trim@ibm.com
            *   references:
                https://pypi.org/project/slackclient/
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/453
        """
        BaseObject.__init__(self, __name__)
        self.payload = payload
        self.event = payload['data']

    def _attr(self,
              key_name: str):
        if key_name in self.event:
            return self.event[key_name]

        if 'message' in self.event:
            if key_name in self.event['message']:
                return self.event['message'][key_name]

        self.logger.warning("\n".join([
            pprint.pformat(self.payload)]))
        raise NotImplementedError("\n".join([
            "Unexpected Message Event Structure (key={})".format(
                key_name)]))

    def web_client(self) -> WebClient:
        return self.payload['web_client']

    def rtm_client(self) -> RTMClient:
        return self.payload['rtm_client']

    def user(self):
        return self._attr("user")

    def text(self):
        return self._attr("text")

    def channel(self):
        return self.event['channel']

    def ts(self):
        return self.event['ts']
