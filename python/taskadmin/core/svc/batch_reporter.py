#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import os

import requests


from base import BaseObject


LOGS_URL = 'https://app.us-south.logging.cloud.ibm.com/b16103d7bc/logs/view?apps=rq-worker'


class BatchReporter(BaseObject):

    def __init__(self):
        BaseObject.__init__(self, self.__class__.__name__)
        self.url = os.getenv('SLACK_URL')

    def post(self, message: str):

        message = f'{message}\nMore details in <{LOGS_URL}>'
        log = 'Posting to slack:' if self.url else 'Not posting to slack:'
        log = f'{log} [{message}]'
        self.logger.info(log)
        if self.url:
            payload = {
                # 'channel': '@xavier.verges',
                'text': message
            }
            response = requests.post(self.url, json=payload)
            if response.status_code != 200:
                self.logger.error('Slack notification failed')
