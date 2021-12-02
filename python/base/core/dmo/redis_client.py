# !/usr/bin/env python
# -*- coding: UTF-8 -*-

import os
import re
import redis


class RedisClient(object):
    """ Redis Client """

    WIKI_AUGMENTED_DB = 3

    WIKI_PAGE_DB = 4

    WIKI_SEARCH_DB = 5

    def __init__(self,
                 db: int = 0,
                 decode_responses: bool = True):
        """
        Created:
            29-May-2019
            craig.trim@ibm.com
        Updated:
            04-Dec-2019
            xavier.verges@es.ibm.com
            *    Use CredentialsFromJson
        Updated:
            23-Jan-2020
            xavier.verges@es.ibm.com
            *    Honor de DB parameter even when using an url. from_url ignores
                 its param if already specified in the url
        """
        from ..dto import CredentialsFromJson

        url = None
        ca_file = None
        if 'REDIS_JSON_CREDENTIALS' in os.environ:
            credentials = CredentialsFromJson(os.environ['REDIS_JSON_CREDENTIALS'],
                                              'rediss')
            url = credentials.url
            ca_file = credentials.ca_file
        if not url:
            url = 'redis://localhost:6379/0'
        url = re.sub(r'/\d*$', f'/{db}', url)
        options = {
            'decode_responses': decode_responses
        }
        if url.startswith('rediss:') and ca_file:
            options['ssl_ca_certs'] = ca_file
        self.redis = redis.from_url(url, **options)
        self.url = CredentialsFromJson.sanitize_url(url, 'rediss')

    def size(self) -> int:
        return self.redis.dbsize()

    def clear(self):
        for key in self.redis.keys():
            self.redis.delete(key)

    def set(self,
            a_key: str,
            value: str) -> None:
        self.redis.set(a_key, value)

    def get(self,
            key) -> str:
        return self.redis.get(key)

    def has(self,
            key) -> bool:
        return self.redis.exists(key)

    def set_list(self,
                 a_key: str,
                 a_list: list) -> None:
        if not self.has(a_key):
            self.redis.rpush(a_key, *a_list)

    def get_list(self,
                 a_key: str) -> list:
        return self.redis.lrange(a_key, 0, 9999999)  # I don't like this either ...

    def set_dict(self,
                 a_key: str,
                 a_dict: dict) -> None:
        if not self.has(a_key):
            self.redis.hmset(a_key, a_dict)

    def get_dict(self,
                 a_key: str) -> dict:
        return self.redis.hgetall(a_key)
