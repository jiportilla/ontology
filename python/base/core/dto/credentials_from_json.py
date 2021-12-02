#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import base64
import json
import os
import re
import warnings

from ..dmo import BaseObject

class CredentialsFromJson(BaseObject):
    _protocols = {
        'mongodb': {
            'env_var': 'MONGO_JSON_CREDENTIALS',
            'sanitizer': re.compile(r'^mongodb://([^:]+):([^@]+)@.*')
        },
        'rediss': {
            'env_var': 'REDIS_JSON_CREDENTIALS',
            'sanitizer': re.compile(r'^rediss://([^:]+):([^@]+)@.*')
        }
    }
    def __init__(self,
                 json: str,
                 protocol: str):
        BaseObject.__init__(self, __name__)
        if not protocol in self._protocols:
            raise ValueError(f'protocol not in {list(self._protocols.keys())}')
        self.url, self.ca_file = self._parse_json(json, protocol)
        self.sanitized_url =  self.sanitize_url(self.url, protocol) if self.url else ''

    def _parse_json(self,
                    env_var: str,
                    protocol: str):
        ca_file = None
        url = None
        try:
            credentials = json.loads(env_var, encoding="utf-8")
            if 'connection' in credentials:
                credentials = credentials['connection']
                if protocol in credentials:
                    credentials = credentials[protocol]
                    if 'composed' in credentials:
                        url = credentials['composed'][0]
                        if 'certificate' in credentials:
                            certificate = credentials['certificate']['certificate_base64']
                            certificate_bytes = base64.b64decode(certificate)
                            certificate = certificate_bytes.decode()
                            ca_file = os.path.join(os.environ["CODE_BASE"],
                                                   "resources/output",
                                                   credentials['certificate']['name'] + '.pem')
                            if not os.path.isfile(ca_file):
                                with open(ca_file, 'w') as f:
                                    f.write(certificate)
            elif 'warning' in credentials:
                warnings.warn(f'{self._protocols[protocol]["env_var"]} {credentials["warning"]}')
            else:
                warnings.warn(f'{self._protocols[protocol]["env_var"]} needs to be defined')
        except Exception as xcpt:
            self.logger.error(f"Exception while parsing {protocol} credentials", xcpt)
        return (url, ca_file)

    @classmethod
    def sanitize_url(cls, url: str, protocol: str) -> str:
        sanitizer = cls._protocols[protocol]['sanitizer']
        match_obj = sanitizer.match(url)                    # type: ignore
        if match_obj:
            for match in match_obj.groups():
                url = url.replace(match, 'xxx', 1)
        return url
