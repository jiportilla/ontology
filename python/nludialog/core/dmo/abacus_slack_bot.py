#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import os

import slack
from slack import RTMClient

from base import BaseObject


class AbacusSlackBot(BaseObject):
    _bot_id = None
    _bot_name = None
    _rtm_client = None

    def __init__(self,
                 bot_name: str = "marvin"):
        """
        Created:
            18-Jul-2019
            craig.trim@ibm.com
            *   references:
                https://pypi.org/project/slackclient/
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/453
        """
        BaseObject.__init__(self, __name__)
        self._bot_name = "Marvin"
        self._bot_id = "ULLURKNFR"

        self._rtm_client = self._start_rtm_client(
            key_name="SLACK_KEY_MARVIN")

    @staticmethod
    def _start_rtm_client(key_name: str) -> RTMClient:
        slack_token = os.environ[key_name]
        rtm_client = slack.RTMClient(token=slack_token)
        rtm_client.start()

        return rtm_client

    def client(self) -> RTMClient:
        return self._rtm_client
